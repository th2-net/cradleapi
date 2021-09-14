/*
 * Copyright 2020-2021 Exactpro (Exactpro Systems Limited)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.exactpro.cradle.cassandra;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.MappedAsyncPagingIterable;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.Row;
import com.exactpro.cradle.*;
import com.exactpro.cradle.cassandra.connection.CassandraConnection;
import com.exactpro.cradle.cassandra.connection.CassandraConnectionSettings;
import com.exactpro.cradle.cassandra.dao.BookOperators;
import com.exactpro.cradle.cassandra.dao.CassandraDataMapper;
import com.exactpro.cradle.cassandra.dao.CassandraDataMapperBuilder;
import com.exactpro.cradle.cassandra.dao.CradleOperators;
import com.exactpro.cradle.cassandra.dao.books.BookEntity;
import com.exactpro.cradle.cassandra.dao.books.PageEntity;
import com.exactpro.cradle.cassandra.dao.cache.CachedScope;
import com.exactpro.cradle.cassandra.dao.cache.CachedSessionDate;
import com.exactpro.cradle.cassandra.dao.cache.CachedTestEventDate;
import com.exactpro.cradle.cassandra.dao.messages.*;
import com.exactpro.cradle.cassandra.dao.testevents.*;
import com.exactpro.cradle.cassandra.iterators.PagedIterator;
import com.exactpro.cradle.cassandra.keyspaces.BookKeyspaceCreator;
import com.exactpro.cradle.cassandra.keyspaces.CradleInfoKeyspaceCreator;
import com.exactpro.cradle.cassandra.resultset.CassandraCradleResultSet;
import com.exactpro.cradle.cassandra.utils.CassandraTimeUtils;
import com.exactpro.cradle.cassandra.utils.QueryExecutor;
import com.exactpro.cradle.intervals.IntervalsWorker;
import com.exactpro.cradle.messages.*;
import com.exactpro.cradle.resultset.CradleResultSet;
import com.exactpro.cradle.testevents.StoredTestEvent;
import com.exactpro.cradle.testevents.StoredTestEventId;
import com.exactpro.cradle.testevents.TestEventFilter;
import com.exactpro.cradle.testevents.TestEventToStore;
import com.exactpro.cradle.utils.CradleStorageException;
import com.exactpro.cradle.utils.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.function.Function;

import static com.exactpro.cradle.cassandra.StorageConstants.*;

public class CassandraCradleStorage extends CradleStorage
{
	private Logger logger = LoggerFactory.getLogger(CassandraCradleStorage.class);
	
	private final CassandraConnection connection;
	private final CassandraStorageSettings settings;
	private final CassandraSemaphore semaphore;
	
	private CradleOperators ops;
	private QueryExecutor exec;
	private Function<BoundStatementBuilder, BoundStatementBuilder> writeAttrs,
			readAttrs,
			strictReadAttrs;
	
	public CassandraCradleStorage(CassandraConnectionSettings connectionSettings, CassandraStorageSettings storageSettings) throws CradleStorageException
	{
		this.connection = new CassandraConnection(connectionSettings);
		this.settings = storageSettings;
		this.semaphore = new CassandraSemaphore(storageSettings.getMaxParallelQueries());
	}

	private static final Consumer<Object> NOOP = whatever -> {};
	
	@Override
	protected void doInit(boolean prepareStorage) throws CradleStorageException
	{
		connectToCassandra();
		
		try
		{
			exec = new QueryExecutor(connection.getSession(),
					settings.getTimeout(), settings.getWriteConsistencyLevel(), settings.getReadConsistencyLevel());
			
			if (prepareStorage)
				createStorage();
			else
				logger.info("Storage creation skipped");
			
			ops = createOperators(connection.getSession(), settings);
			
			Duration timeout = Duration.ofMillis(settings.getTimeout());
			int resultPageSize = settings.getResultPageSize();
			writeAttrs = builder -> builder.setConsistencyLevel(settings.getWriteConsistencyLevel())
					.setTimeout(timeout);
			readAttrs = builder -> builder.setConsistencyLevel(settings.getReadConsistencyLevel())
					.setTimeout(timeout)
					.setPageSize(resultPageSize);
			strictReadAttrs = builder -> builder.setConsistencyLevel(ConsistencyLevel.ALL)
					.setTimeout(timeout)
					.setPageSize(resultPageSize);
		}
		catch (Exception e)
		{
			throw new CradleStorageException("Could not initialize Cassandra storage", e);
		}
	}
	
	@Override
	protected void doDispose() throws CradleStorageException
	{
		if (connection.isRunning())
		{
			logger.info("Disconnecting from Cassandra...");
			try
			{
				connection.stop();
			}
			catch (Exception e)
			{
				throw new CradleStorageException("Error while closing Cassandra connection", e);
			}
		}
		else
			logger.info("Already disconnected from Cassandra");
	}
	
	@Override
	protected Collection<BookInfo> loadBooks() throws CradleStorageException
	{
		Collection<BookInfo> result = new ArrayList<>();
		for (BookEntity bookEntity : ops.getCradleBookOperator().getAll(readAttrs))
		{
			BookId bookId = new BookId(bookEntity.getName());
			BookOperators bookOp = ops.addOperators(bookId, bookEntity.getKeyspaceName());
			
			Collection<PageInfo> pages = new ArrayList<>();
			for (PageEntity pageEntity : bookOp.getPageOperator().getAll(readAttrs))
				pages.add(pageEntity.toPageInfo(bookId));
			
			result.add(bookEntity.toBookInfo(pages));
		}
		return result;
	}
	
	@Override
	protected void doAddBook(BookInfo newBook) throws CradleStorageException
	{
		BookEntity bookEntity = new BookEntity(newBook);
		createBookKeyspace(bookEntity);
		ops.getCradleBookOperator().write(bookEntity, writeAttrs);
		ops.addOperators(newBook.getId(), bookEntity.getKeyspaceName());
	}
	
	@Override
	protected void doSwitchToNextPage(BookId bookId, String pageName, Instant timestamp) throws CradleStorageException
	{
		PageEntity entity = new PageEntity(pageName, timestamp, null);
		ops.getOperators(bookId).getPageOperator().write(entity, writeAttrs);
	}


	@Override
	protected void doStoreMessageBatch(MessageBatch batch, PageInfo page) throws IOException
	{
		try
		{
			doStoreMessageBatchAsync(batch, page).get();
		}
		catch (Exception e)
		{
			throw new IOException("Error while storing message batch "+batch.getId(), e);
		}
	}

	@Override
	protected CompletableFuture<Void> doStoreMessageBatchAsync(MessageBatch batch, PageInfo page)
			throws IOException, CradleStorageException
	{
		PageId pageId = page.getId();
		StoredMessageId batchId = batch.getId();
		Collection<MessageBatchEntity> entities = MessageEntityUtils.toEntities(batch, pageId,
				settings.getMaxUncompressedMessageBatchSize(), settings.getMessageBatchChunkSize());
		BookOperators bookOps = ops.getOperators(pageId.getBookId());
		MessageBatchOperator mbOperator = bookOps.getMessageBatchOperator();
		SessionDatesOperator sdOperator = bookOps.getSessionDatesOperator();

		CompletableFuture<MessageBatchEntity> result = CompletableFuture.completedFuture(null);
		for (MessageBatchEntity entity : entities)
			result = result.thenComposeAsync(r -> mbOperator.write(entity, writeAttrs));

		return result
				.thenComposeAsync(r ->
				{
					LocalDateTime ldt = TimeUtils.toLocalTimestamp(batchId.getTimestamp());
					CachedSessionDate cachedSessionDate = new CachedSessionDate(pageId.getName(), ldt.toLocalDate(),
							batchId.getSessionAlias(), batchId.getDirection().getLabel(),
							CassandraTimeUtils.getPart(ldt));
					if (!bookOps.getSessionDatesCache().store(cachedSessionDate))
					{
						logger.debug("Skipped writing session date of message batch '{}'", batchId);
						return CompletableFuture.completedFuture(null);
					}

					logger.debug("Writing session date of batch '{}'", batchId);
					return sdOperator.write(new SessionDatesEntity(batchId, pageId), writeAttrs);
				})
				.thenAccept(NOOP);
	}
	
	
	@Override
	protected void doStoreTestEvent(TestEventToStore event, PageInfo page) throws IOException
	{
		try
		{
			doStoreTestEventAsync(event, page).get();
		}
		catch (Exception e)
		{
			throw new IOException("Error while storing test event "+event.getId(), e);
		}
	}

	@Override
	protected CompletableFuture<Void> doStoreTestEventAsync(TestEventToStore event, PageInfo page) throws IOException, CradleStorageException
	{
		PageId pageId = page.getId();
		List<TestEventEntity> entities = EventEntityUtils.toEntities(event, pageId, 
				settings.getMaxUncompressedTestEventSize(), settings.getTestEventChunkSize(), settings.getTestEventMessagesPerChunk());
		BookOperators bookOps = ops.getOperators(pageId.getBookId());
		TestEventOperator op = bookOps.getTestEventOperator();
		
		CompletableFuture<TestEventEntity> result = null;
		for (TestEventEntity ent : entities)
		{
			if (result == null)
				result = op.write(ent, writeAttrs);
			else
				result = result.thenComposeAsync(r -> op.write(ent, writeAttrs));
		}
		return result
				.thenComposeAsync(r -> {
					if (!bookOps.getScopesCache().store(new CachedScope(pageId.toString(), event.getScope())))
					{
						logger.debug("Skipped writing scope of event '{}'", event.getId());
						return CompletableFuture.completedFuture(null);
					}
					
					logger.debug("Writing scope of event '{}'", event.getId());
					return bookOps.getScopeOperator()
							.write(new ScopeEntity(pageId.getName(), event.getScope()), writeAttrs);
				})
				.thenComposeAsync(r -> {
					LocalDateTime ldt = TimeUtils.toLocalTimestamp(event.getStartTimestamp());
					if (!bookOps.getEventDatesCache().store(new CachedTestEventDate(pageId.toString(), ldt.toLocalDate(), event.getScope(), CassandraTimeUtils.getPart(ldt))))
					{
						logger.debug("Skipped writing start date of event '{}'", event.getId());
						return CompletableFuture.completedFuture(null);
					}
					
					logger.debug("Writing start date of event '{}'", event.getId());
					return bookOps.getEventDateOperator()
							.write(new EventDateEntity(pageId.getName(), ldt.toLocalDate(), event.getScope(), CassandraTimeUtils.getPart(ldt)), writeAttrs);
				})
				.thenAccept(NOOP);
	}

	@Override
	protected void doUpdateParentTestEvents(TestEventToStore event) throws IOException
	{
		if (event.isSuccess())
			return;
		
		try
		{
			doUpdateParentTestEventsAsync(event).get();
		}
		catch (Exception e)
		{
			throw new IOException("Error while updating parents of "+event.getId()+" test event", e);
		}
	}

	@Override
	protected CompletableFuture<Void> doUpdateParentTestEventsAsync(TestEventToStore event)
	{
		if (event.isSuccess())
			return CompletableFuture.completedFuture(null);
		
		return failEventAndParents(event.getParentId());
	}
	
	@Override
	protected void doUpdateEventStatus(StoredTestEvent event, boolean success) throws IOException
	{
		try
		{
			doUpdateEventStatusAsync(event, success).get();
		}
		catch (Exception e)
		{
			throw new IOException("Error while updating status of event "+event.getId(), e);
		}
	}

	@Override
	protected CompletableFuture<Void> doUpdateEventStatusAsync(StoredTestEvent event, boolean success)
	{
		//TODO: implement
		return null;
	}
	

	@Override
	protected StoredMessage doGetMessage(StoredMessageId id, PageId pageId) throws IOException
	{
		try
		{
			return doGetMessageAsync(id, pageId).get();
		}
		catch (Exception e)
		{
			throw new IOException("Error while getting message "+id, e);
		}
	}

	@Override
	protected CompletableFuture<StoredMessage> doGetMessageAsync(StoredMessageId id, PageId pageId)
	{
		return null; //TODO: implement readMessage(id, true);
	}

	@Override
	protected Collection<StoredMessage> doGetMessageBatch(StoredMessageId id, PageId pageId) throws IOException
	{
		try
		{
			return doGetMessageBatchAsync(id, pageId).get();
		}
		catch (Exception e)
		{
			throw new IOException("Error while getting message batch "+id, e);
		}
	}

	@Override
	protected CompletableFuture<Collection<StoredMessage>> doGetMessageBatchAsync(StoredMessageId id, PageId pageId)
			throws CradleStorageException
	{
		logger.debug("Getting message batch for message with id '{}'", id);
		BookId bookId = pageId.getBookId();
		LocalDateTime ldt = TimeUtils.toLocalTimestamp(id.getTimestamp());
		BookOperators bookOps = ops.getOperators(bookId);
		return bookOps.getMessageBatchOperator()
				.getNearestTimeAndSequenceBefore(pageId.getName(), id.getSessionAlias(), id.getDirection().getLabel(),
						CassandraTimeUtils.getPart(ldt), ldt.toLocalDate(), ldt.toLocalTime(), id.getSequence(),
						readAttrs)
				.thenComposeAsync(row ->
				{
					if (row == null)
					{
						logger.debug("No message batches found by id '{}'", id);
						return null;
					}
					return bookOps
							.getMessageBatchOperator()
							.get(pageId.getName(), id.getSessionAlias(), id.getDirection().getLabel(),
									CassandraTimeUtils.getPart(ldt), ldt.toLocalDate(),
									row.getLocalTime(MESSAGE_TIME),
									row.getLong(SEQUENCE), readAttrs)
							.thenApplyAsync(e ->
							{
								try
								{
									StoredMessageBatch batch = MessageEntityUtils.toStoredMessageBatch(e, pageId);
									logger.debug("Message batch with id '{}' found for message with id '{}'", batch.getId(), id);
									return batch.getMessages();
								}
								catch (Exception ex)
								{
									throw new CompletionException(ex);
								}
							});
				});
	}
	
	
	@Override
	protected Iterable<StoredMessage> doGetMessages(StoredMessageFilter filter) throws IOException
	{
		try
		{
			return doGetMessagesAsync(filter).get();
		}
		catch (Exception e)
		{
			throw new IOException("Error while getting messages filtered by "+filter, e);
		}
	}
	
	@Override
	protected CompletableFuture<Iterable<StoredMessage>> doGetMessagesAsync(StoredMessageFilter filter)
	{
		return null;  //TODO: implement
	}
	
	@Override
	protected Iterable<MessageBatch> doGetMessagesBatches(StoredMessageFilter filter) throws IOException
	{
		try
		{
			return doGetMessagesBatchesAsync(filter).get();
		}
		catch (Exception e)
		{
			throw new IOException("Error while getting messages filtered by "+filter, e);
		}
	}
	
	@Override
	protected CompletableFuture<Iterable<MessageBatch>> doGetMessagesBatchesAsync(StoredMessageFilter filter)
	{
		return null; //TODO: implement
	}
	
	
	@Override
	protected long doGetLastSequence(String sessionAlias, Direction direction, BookId bookId)
			throws IOException, CradleStorageException
	{
		
		BookOperators bookOps = ops.getOperators(bookId);
		MessageBatchOperator mbOp = bookOps.getMessageBatchOperator();
		SessionDatesOperator sdOp = bookOps.getSessionDatesOperator();
		BookInfo book = bpc.getBook(bookId);
		PageInfo currentPage = bpc.getActivePage(bookId);
		String lastPart = null;
		while (lastPart == null && currentPage != null)
		{
			List<LocalDate> localDates = TimeUtils.splitByDate(currentPage.getStarted(), currentPage.getEnded());
			try
			{
				lastPart = sdOp.getLastPart(currentPage.getId().getName(), localDates,
								sessionAlias, direction.getLabel(), readAttrs).get();
			}
			catch (InterruptedException | ExecutionException e)
			{
				String msg = String.format(
						"Error occurs while getting last part for page '%s', message dates %s, session alias '%s', " +
								"direction '%s'", currentPage.getId().getName(), localDates, sessionAlias, direction);
				throw new CradleStorageException(msg, e);
			}
			
			if (lastPart == null)
				currentPage = book.getPreviousPage(currentPage.getStarted());
		}
		if (lastPart == null || currentPage == null)
		{
			logger.debug("There is no messages yet in book '{}' with session alias '{}' and direction '{}'", bookId,
					sessionAlias, direction);
			return 0L;
		}

		try
		{
			Row row = mbOp.getLastSequence(currentPage.getId().getName(), sessionAlias, direction.getLabel(), lastPart,
					readAttrs).get();
			return row == null ? 0L : row.getLong(LAST_SEQUENCE);
		}
		catch (InterruptedException | ExecutionException e)
		{
			String msg = String.format(
					"Error occurs while getting last sequence for page '%s', session alias '%s', part '%s', " +
							"direction '%s'", currentPage.getId().getName(), sessionAlias, lastPart, direction);
			throw new CradleStorageException(msg, e);
		}
	}
	
	@Override
	protected Collection<String> doGetSessionAliases(PageId pageId) throws IOException
	{
		List<String> result = new ArrayList<>();
		//TODO: implement
//		for (StreamEntity entity : ops.getMessageBatchOperator().getStreams(readAttrs))
//		{
//			//if (instanceUuid.equals(entity.getInstanceId()))
//				result.add(entity.getStreamName());
//		}
//		result.sort(null);
		return result;
	}
	
	
	@Override
	protected StoredTestEvent doGetTestEvent(StoredTestEventId id, PageId pageId) throws IOException
	{
		try
		{
			return doGetTestEventAsync(id, pageId).get();
		}
		catch (Exception e)
		{
			throw new IOException("Could not get test event", e);
		}
	}

	@Override
	protected CompletableFuture<StoredTestEvent> doGetTestEventAsync(StoredTestEventId id, PageId pageId) throws CradleStorageException
	{
		LocalDateTime ldt = TimeUtils.toLocalTimestamp(id.getStartTimestamp());
		LocalTime lt = ldt.toLocalTime();
		BookId bookId = pageId.getBookId();
		return ops.getOperators(bookId).getTestEventOperator().get(pageId.getName(), ldt.toLocalDate(), id.getScope(), CassandraTimeUtils.getPart(ldt), 
						lt, id.getId(), readAttrs)
				.thenApplyAsync(r -> {
					try
					{
						return EventEntityUtils.toStoredTestEvent(r, pageId);
					}
					catch (Exception e)
					{
						throw new CompletionException("Error while converting data of event "+id+" into test event", e);
					}
				});
	}
	
	
	@Override
	protected CradleResultSet<StoredTestEvent> doGetTestEvents(TestEventFilter filter, BookInfo book) throws CradleStorageException, IOException
	{
		try
		{
			return doGetTestEventsAsync(filter, book).get();
		}
		catch (Exception e)
		{
			throw new IOException("Error while getting test events filtered by "+filter, e);
		}
	}
	
	@Override
	protected CompletableFuture<CradleResultSet<StoredTestEvent>> doGetTestEventsAsync(TestEventFilter filter, BookInfo book) 
			throws CradleStorageException, IOException
	{
		TestEventIteratorProvider provider = new TestEventIteratorProvider("get test events filtered by "+filter, 
				filter, ops.getOperators(filter.getBookId()), book, readAttrs);
		return provider.nextIterator()
				.thenApplyAsync(r -> new CassandraCradleResultSet<>(r, provider));
	}
	
	
	@Override
	protected Collection<String> doGetScopes(BookId bookId) throws IOException, CradleStorageException
	{
		MappedAsyncPagingIterable<ScopeEntity> entities;
		try
		{
			entities = ops.getOperators(bookId).getScopeOperator().all(readAttrs).get();
		}
		catch (Exception e)
		{
			throw new IOException("Error while getting scopes for book '"+bookId+"'", e);
		}
		
		if (entities == null)
			return Collections.emptySet();
		
		Collection<String> result = new HashSet<>();
		PagedIterator<ScopeEntity> it = new PagedIterator<>(entities);
		while (it.hasNext())
			result.add(it.next().getScope());
		return result;
	}
	
	
	@Override
	public IntervalsWorker getIntervalsWorker(PageId pageId)
	{
		return null; //TODO: implement
	}
	
	
	protected void connectToCassandra() throws CradleStorageException
	{
		if (!connection.isRunning())
		{
			logger.info("Connecting to Cassandra...");
			try
			{
				connection.start();
				logger.info("Connected to Cassandra");
			}
			catch (Exception e)
			{
				throw new CradleStorageException("Could not open Cassandra connection", e);
			}
		}
		else
			logger.info("Already connected to Cassandra");
	}
	
	protected CradleOperators createOperators(CqlSession session, CassandraStorageSettings settings)
	{
		CassandraDataMapper dataMapper = new CassandraDataMapperBuilder(session).build();
		return new CradleOperators(dataMapper, settings);
	}
	
	protected void createStorage() throws CradleStorageException
	{
		try
		{
			logger.info("Creating storage");
			new CradleInfoKeyspaceCreator(exec, settings).createAll();
			logger.info("Storage creation finished");
		}
		catch (IOException e)
		{
			throw new CradleStorageException("Error while creating storage", e);
		}
	}
	
	protected void createBookKeyspace(BookEntity bookEntity) throws CradleStorageException
	{
		String name = bookEntity.getName();
		try
		{
			logger.info("Creating storage for book '{}'", name);
			new BookKeyspaceCreator(bookEntity.getKeyspaceName(), exec, settings).createAll();;
			logger.info("Storage creation for book '{}' finished", name);
		}
		catch (IOException e)
		{
			throw new CradleStorageException("Error while creating storage for book '"+name+"'", e);
		}
	}
	
	protected CassandraStorageSettings getSettings()
	{
		return settings;
	}
	
	protected QueryExecutor getQueryExecutor()
	{
		return exec;
	}
	
	protected CassandraSemaphore getSemaphore()
	{
		return semaphore;
	}
	
	public Function<BoundStatementBuilder, BoundStatementBuilder> getWriteAttrs()
	{
		return writeAttrs;
	}
	
	public Function<BoundStatementBuilder, BoundStatementBuilder> getReadAttrs()
	{
		return readAttrs;
	}
	
	public Function<BoundStatementBuilder, BoundStatementBuilder> getStrictReadAttrs()
	{
		return strictReadAttrs;
	}
	
	
//TODO: implement
//	private CompletableFuture<DetailedMessageBatchEntity> readMessageBatchEntity(StoredMessageId messageId, boolean rawMessage)
//	{
//		MessageBatchOperator op = rawMessage ? ops.getMessageBatchOperator() : ops.getProcessedMessageBatchOperator();
//		return CassandraMessageUtils.getMessageBatch(messageId, op, semaphore, instanceUuid, readAttrs);
//	}

//TODO: implement
//	private CompletableFuture<StoredMessage> readMessage(StoredMessageId id, boolean rawMessage)
//	{
//		CompletableFuture<DetailedMessageBatchEntity> entityFuture = readMessageBatchEntity(id, rawMessage);
//		return entityFuture.thenCompose((entity) -> {
//			if (entity == null)
//				return CompletableFuture.completedFuture(null);
//			StoredMessage msg;
//			try
//			{
//				msg = MessageUtils.bytesToOneMessage(entity.getContent(), entity.isCompressed(), id);
//			}
//			catch (IOException e)
//			{
//				throw new CompletionException("Error while reading message", e);
//			}
//			return CompletableFuture.completedFuture(msg);
//		});
//	}

	private void checkTimeBoundaries(LocalDateTime fromDateTime, LocalDateTime toDateTime, Instant originalFrom, Instant originalTo)
			throws CradleStorageException
	{
		LocalDate fromDate = fromDateTime.toLocalDate(),
				toDate = toDateTime.toLocalDate();
		if (!fromDate.equals(toDate))
			throw new CradleStorageException("Left and right boundaries should be of the same date, but got '"+originalFrom+"' and '"+originalTo+"'");
	}

	

	protected CompletableFuture<Void> failEventAndParents(StoredTestEventId eventId)
	{
		return CompletableFuture.completedFuture(null);
		//TODO: implement
//		return getTestEventAsync(eventId)
//				.thenComposeAsync((event) -> {
//					if (event == null || !event.isSuccess())  //Invalid event ID or event is already failed, which means that its parents are already updated
//						return CompletableFuture.completedFuture(null);
//
//					CompletableFuture<Void> update = doUpdateEventStatusAsync(event, false);
//					if (event.getParentId() != null)
//						return update.thenComposeAsync((u) -> failEventAndParents(event.getParentId()));
//					return update;
//				});
	}
}

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
import com.datastax.oss.driver.api.core.PagingIterable;
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
import com.exactpro.cradle.cassandra.dao.books.PageNameEntity;
import com.exactpro.cradle.cassandra.dao.books.PageNameOperator;
import com.exactpro.cradle.cassandra.dao.books.PageOperator;
import com.exactpro.cradle.cassandra.dao.cache.CachedPageSession;
import com.exactpro.cradle.cassandra.dao.cache.CachedSession;
import com.exactpro.cradle.cassandra.dao.messages.*;
import com.exactpro.cradle.cassandra.dao.testevents.PageScopeEntity;
import com.exactpro.cradle.cassandra.dao.testevents.PageScopesOperator;
import com.exactpro.cradle.cassandra.dao.testevents.ScopeEntity;
import com.exactpro.cradle.cassandra.dao.testevents.TestEventEntity;
import com.exactpro.cradle.cassandra.dao.testevents.TestEventOperator;
import com.exactpro.cradle.cassandra.iterators.PagedIterator;
import com.exactpro.cradle.cassandra.keyspaces.BookKeyspaceCreator;
import com.exactpro.cradle.cassandra.keyspaces.CradleInfoKeyspaceCreator;
import com.exactpro.cradle.cassandra.resultset.CassandraCradleResultSet;
import com.exactpro.cradle.cassandra.utils.QueryExecutor;
import com.exactpro.cradle.intervals.IntervalsWorker;
import com.exactpro.cradle.messages.*;
import com.exactpro.cradle.resultset.CradleResultSet;
import com.exactpro.cradle.testevents.StoredTestEvent;
import com.exactpro.cradle.testevents.TestEventFilter;
import com.exactpro.cradle.testevents.StoredTestEventId;
import com.exactpro.cradle.testevents.TestEventToStore;
import com.exactpro.cradle.utils.CradleStorageException;
import com.exactpro.cradle.utils.TimeUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.function.Function;

import static com.exactpro.cradle.cassandra.StorageConstants.*;

public class CassandraCradleStorage extends CradleStorage
{
	private Logger logger = LoggerFactory.getLogger(CassandraCradleStorage.class);
	
	private final CassandraConnection connection;
	private final CassandraStorageSettings settings;
	
	private CradleOperators ops;
	private QueryExecutor exec;
	private Function<BoundStatementBuilder, BoundStatementBuilder> writeAttrs,
			readAttrs,
			strictReadAttrs;
	private EventsWorker eventsWorker;
	
	public CassandraCradleStorage(CassandraConnectionSettings connectionSettings, CassandraStorageSettings storageSettings, 
			ExecutorService composingService) throws CradleStorageException
	{
		super(composingService);
		this.connection = new CassandraConnection(connectionSettings);
		this.settings = storageSettings;
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
			
			eventsWorker = new EventsWorker(settings, ops, composingService, writeAttrs, readAttrs);
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
	protected Collection<BookInfo> loadBooks() throws IOException
	{
		Collection<BookInfo> result = new ArrayList<>();
		try
		{
			for (BookEntity bookEntity : ops.getCradleBookOperator().getAll(readAttrs))
			{
				BookId bookId = new BookId(bookEntity.getName());
				ops.addOperators(bookId, bookEntity.getKeyspaceName());
				Collection<PageInfo> pages = loadPageInfo(bookId);
				
				result.add(bookEntity.toBookInfo(pages));
			}
		}
		catch (Exception e)
		{
			throw new IOException("Error while loading books", e);
		}
		return result;
	}
	
	@Override
	protected void doAddBook(BookToAdd newBook, BookId bookId) throws IOException
	{
		BookEntity bookEntity = new BookEntity(newBook);
		createBookKeyspace(bookEntity);
		
		try
		{
			if (!ops.getCradleBookOperator().write(bookEntity, writeAttrs).wasApplied())
				throw new IOException("Query to insert book '"+bookEntity.getName()+"' was not applied. Probably, book already exists");
		}
		catch (IOException e)
		{
			throw e;
		}
		catch (Exception e)
		{
			throw new IOException("Error while writing info of book '"+bookId+"'", e);
		}
		ops.addOperators(bookId, bookEntity.getKeyspaceName());
	}
	
	@Override
	protected void doSwitchToNewPage(BookId bookId, String pageName, Instant timestamp, String comment, PageInfo prevPage) throws CradleStorageException, IOException
	{
		BookOperators bookOps = ops.getOperators(bookId);
		PageOperator pageOp = bookOps.getPageOperator();
		PageNameOperator pageNameOp = bookOps.getPageNameOperator();
		try
		{
			PageNameEntity nameEntity = new PageNameEntity(bookId.getName(), pageName, timestamp, comment, null);
			if (!pageNameOp.writeNew(nameEntity, writeAttrs).wasApplied())
				throw new IOException("Query to insert page '"+nameEntity.getName()+"' was not applied. Probably, page already exists");
			PageEntity entity = new PageEntity(bookId.getName(), pageName, timestamp, comment, null);
			pageOp.write(entity, writeAttrs);
			
			if (prevPage != null)
			{
				pageOp.update(new PageEntity(prevPage), writeAttrs);
				pageNameOp.update(new PageNameEntity(prevPage), writeAttrs);
			}
		}
		catch (IOException e)
		{
			throw e;
		}
		catch (Exception e)
		{
			throw new IOException("Error while writing page info", e);
		}
	}
	
	@Override
	protected Collection<PageInfo> doLoadPages(BookId bookId) throws CradleStorageException, IOException
	{
		return loadPageInfo(bookId);
	}
	
	@Override
	protected void doRemovePage(PageInfo page) throws CradleStorageException, IOException
	{
		PageId pageId = page.getId();
		BookOperators bookOps = ops.getOperators(pageId.getBookId());
		
		removeMessages(pageId, bookOps);
		removeTestEvents(pageId, bookOps);
		removePage(page, bookOps);
	}
	
	
	@Override
	protected void doStoreMessageBatch(MessageBatchToStore batch, PageInfo page) throws IOException
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
	protected CompletableFuture<Void> doStoreMessageBatchAsync(MessageBatchToStore batch, PageInfo page)
			throws IOException, CradleStorageException
	{
		PageId pageId = page.getId();
		StoredMessageId batchId = batch.getId();
		Collection<MessageBatchEntity> entities = MessageEntityUtils.toEntities(batch, pageId,
				settings.getMaxUncompressedMessageBatchSize(), settings.getMessageBatchChunkSize());
		BookOperators bookOps = ops.getOperators(pageId.getBookId());
		MessageBatchOperator mbOperator = bookOps.getMessageBatchOperator();
		PageSessionsOperator psOperator = bookOps.getPageSessionsOperator();
		SessionsOperator sOperator = bookOps.getSessionsOperator();

		CompletableFuture<MessageBatchEntity> result = CompletableFuture.completedFuture(null);
		for (MessageBatchEntity entity : entities)
			result = result.thenComposeAsync(r -> mbOperator.write(entity, writeAttrs), composingService);

		String sessionAlias = batchId.getSessionAlias();
		return result
				.thenComposeAsync(r ->
				{
					CachedPageSession cachedPageSession = new CachedPageSession(pageId.toString(),
							sessionAlias, batchId.getDirection().getLabel());
					if (!bookOps.getPageSessionsCache().store(cachedPageSession))
					{
						logger.debug("Skipped writing page/session of message batch '{}'", batchId);
						return CompletableFuture.completedFuture(null);
					}

					logger.debug("Writing page/session of batch '{}'", batchId);
					return psOperator.write(new PageSessionEntity(batchId, pageId), writeAttrs);
				}, composingService)
				.thenComposeAsync(r ->{
					String book = batchId.getBookId().getName();
					CachedSession cachedSession = new CachedSession(book, sessionAlias);
					if (!bookOps.getSessionsCache().store(cachedSession))
					{
						logger.debug("Skipped writing book/session of message batch '{}'", batchId);
						return CompletableFuture.completedFuture(null);
					}
					logger.debug("Writing book/session of batch '{}'", batchId);
					return sOperator.write(new SessionEntity(book, sessionAlias), writeAttrs);
				}, composingService)
				.thenAccept(NOOP);
	}
	
	
	@Override
	protected void doStoreTestEvent(TestEventToStore event, PageInfo page) throws IOException, CradleStorageException
	{
		PageId pageId = page.getId();
		BookId bookId = pageId.getBookId();
		BookOperators bookOps = ops.getOperators(bookId);
		try
		{
			List<TestEventEntity> entities = eventsWorker.createEntities(event, pageId);
			eventsWorker.storeEntities(entities, bookId).get();
			eventsWorker.storeScope(event, bookOps).get();
			eventsWorker.storePageScope(event, pageId, bookOps).get();
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
		BookId bookId = pageId.getBookId();
		BookOperators bookOps = ops.getOperators(bookId);
		return CompletableFuture.supplyAsync(() -> {
					try
					{
						return eventsWorker.createEntities(event, pageId);
					}
					catch (IOException e)
					{
						throw new CompletionException(e);
					}
				}, composingService)
				.thenComposeAsync((entities) -> eventsWorker.storeEntities(entities, bookId), composingService)
				.thenComposeAsync((r) -> eventsWorker.storeScope(event, bookOps), composingService)
				.thenComposeAsync((r) -> eventsWorker.storePageScope(event, pageId, bookOps), composingService)
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
			eventsWorker.updateStatus(event, success).get();
		}
		catch (Exception e)
		{
			throw new IOException("Error while updating status of event "+event.getId(), e);
		}
	}

	@Override
	protected CompletableFuture<Void> doUpdateEventStatusAsync(StoredTestEvent event, boolean success)
	{
		return eventsWorker.updateStatus(event, success);
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
			throws CradleStorageException
	{
		return doGetMessageBatchAsync(id, pageId)
				.thenComposeAsync(msgs ->
				{
					if (msgs == null)
						return CompletableFuture.completedFuture(null);

					Optional<StoredMessage> found = msgs.stream().filter(m -> id.equals(m.getId())).findFirst();
					if (found.isPresent())
						return CompletableFuture.completedFuture(found.get());

					logger.debug("There is no message with id '{}' in batch '{}'", id, msgs.iterator().next().getId());
					return CompletableFuture.completedFuture(null);
				}, composingService);
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
						ldt.toLocalDate(), ldt.toLocalTime(), id.getSequence(), readAttrs)
				.thenComposeAsync(row ->
				{
					if (row == null)
					{
						logger.debug("No message batches found by id '{}'", id);
						return CompletableFuture.completedFuture(null);
					}
					return bookOps
							.getMessageBatchOperator()
							.get(pageId.getName(), id.getSessionAlias(), id.getDirection().getLabel(),
									ldt.toLocalDate(), row.getLocalTime(MESSAGE_TIME), row.getLong(SEQUENCE), readAttrs)
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
							}, composingService);
				}, composingService);
	}
	
	
	@Override
	protected Iterable<StoredMessage> doGetMessages(StoredMessageFilter filter, BookInfo book) throws IOException
	{
		try
		{
			return doGetMessagesAsync(filter, book).get();
		}
		catch (Exception e)
		{
			throw new IOException("Error while getting messages filtered by "+filter, e);
		}
	}
	
	@Override
	protected CompletableFuture<Iterable<StoredMessage>> doGetMessagesAsync(StoredMessageFilter filter, BookInfo book)
			throws CradleStorageException
	{
		StoredMessagesIteratorProvider provider =
				new StoredMessagesIteratorProvider("get messages filtered by " + filter, filter,
						ops.getOperators(book.getId()), book, composingService, readAttrs);
		return provider.nextIterator()
				.thenApply(r -> () -> new CassandraCradleResultSet<>(r, provider));
	}
	
	@Override
	protected CradleResultSet<StoredMessageBatch> doGetMessagesBatches(StoredMessageFilter filter, BookInfo book) throws IOException
	{
		try
		{
			return doGetMessagesBatchesAsync(filter, book).get();
		}
		catch (Exception e)
		{
			throw new IOException("Error while getting message batches filtered by "+filter, e);
		}
	}
	
	@Override
	protected CompletableFuture<CradleResultSet<StoredMessageBatch>> doGetMessagesBatchesAsync(StoredMessageFilter filter, BookInfo book)
			throws CradleStorageException
	{
		MessageBatchIteratorProvider provider =
				new MessageBatchIteratorProvider("get messages batches filtered by " + filter, filter,
						ops.getOperators(book.getId()), book, composingService, readAttrs);
		return provider.nextIterator()
				.thenApply(r -> new CassandraCradleResultSet<>(r, provider));
	}
	
	
	@Override
	protected long doGetLastSequence(String sessionAlias, Direction direction, BookId bookId)
			throws IOException, CradleStorageException
	{
		BookOperators bookOps = ops.getOperators(bookId);
		MessageBatchOperator mbOp = bookOps.getMessageBatchOperator();
		BookInfo book = bpc.getBook(bookId);
		PageInfo currentPage = bpc.getActivePage(bookId);
		Row row = null;
		while (row == null && currentPage != null)
		{
			try
			{
				row = mbOp.getLastSequence(currentPage.getId().getName(), sessionAlias, direction.getLabel(),
						readAttrs).get();
			}
			catch (InterruptedException | ExecutionException e)
			{
				String msg = String.format("Error occurs while getting last sequence for page '%s', session alias '%s', " +
						"direction '%s'", currentPage.getId().getName(), sessionAlias, direction);
				throw new CradleStorageException(msg, e);
			}

			if (row == null)
				currentPage = book.getPreviousPage(currentPage.getStarted());
		}
		if (row == null)
		{
			logger.debug("There is no messages yet in book '{}' with session alias '{}' and direction '{}'", bookId,
					sessionAlias, direction);
			return 0L;
		}

		return row.getLong(LAST_SEQUENCE);
	}
	
	@Override
	protected Collection<String> doGetSessionAliases(BookId bookId) throws IOException, CradleStorageException
	{
		MappedAsyncPagingIterable<SessionEntity> entities;
		try
		{
			entities = ops.getOperators(bookId).getSessionsOperator().get(bookId.getName(), readAttrs).get();
		}
		catch (Exception e)
		{
			throw new CradleStorageException("Error occurred while getting session aliases for book '"+bookId+"'", e);
		}
		
		if (entities == null)
			return Collections.emptySet();
		
		Collection<String> result = new HashSet<>();
		PagedIterator<SessionEntity> it = new PagedIterator<>(entities);
		while (it.hasNext())
			result.add(it.next().getSessionAlias());
		return result;
	}
	
	
	@Override
	protected StoredTestEvent doGetTestEvent(StoredTestEventId id, PageId pageId) throws IOException
	{
		try
		{
			return eventsWorker.getTestEvent(id, pageId).get();
		}
		catch (Exception e)
		{
			throw new IOException("Could not get test event", e);
		}
	}

	@Override
	protected CompletableFuture<StoredTestEvent> doGetTestEventAsync(StoredTestEventId id, PageId pageId) throws CradleStorageException
	{
		return eventsWorker.getTestEvent(id, pageId);
	}
	
	
	@Override
	protected CradleResultSet<StoredTestEvent> doGetTestEvents(TestEventFilter filter, BookInfo book) throws CradleStorageException, IOException
	{
		try
		{
			return eventsWorker.getTestEvents(filter, book).get();
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
		return eventsWorker.getTestEvents(filter, book);
	}
	
	
	@Override
	protected Collection<String> doGetScopes(BookId bookId) throws IOException, CradleStorageException
	{
		MappedAsyncPagingIterable<ScopeEntity> entities;
		try
		{
			entities = ops.getOperators(bookId).getScopeOperator().get(bookId.getName(), readAttrs).get();
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
	
	protected void createBookKeyspace(BookEntity bookEntity) throws IOException
	{
		String name = bookEntity.getName();
		try
		{
			logger.info("Creating storage for book '{}'", name);
			new BookKeyspaceCreator(bookEntity.getKeyspaceName(), exec, settings).createAll();
			logger.info("Storage creation for book '{}' finished", name);
		}
		catch (Exception e)
		{
			throw new IOException("Error while creating storage for book '"+name+"'", e);
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
	
	
	private Collection<PageInfo> loadPageInfo(BookId bookId) throws IOException
	{
		Collection<PageInfo> result = new ArrayList<>();
		try
		{
			for (PageEntity pageEntity : ops.getOperators(bookId).getPageOperator().getAll(bookId.getName(), readAttrs))
			{
				if (pageEntity.getRemoved() == null)
					result.add(pageEntity.toPageInfo());
			}
		}
		catch (Exception e)
		{
			throw new IOException("Error while loading pages of book '"+bookId+"'", e);
		}
		return result;
	}
	
	protected CompletableFuture<Void> failEventAndParents(StoredTestEventId eventId)
	{
		try
		{
			return getTestEventAsync(eventId)
					.thenComposeAsync((event) -> {
						if (event == null || !event.isSuccess())  //Invalid event ID or event is already failed, which means that its parents are already updated
							return CompletableFuture.completedFuture(null);
						
						CompletableFuture<Void> update = doUpdateEventStatusAsync(event, false);
						if (event.getParentId() != null)
							return update.thenComposeAsync((u) -> failEventAndParents(event.getParentId()), composingService);
						return update;
					}, composingService);
		}
		catch (CradleStorageException e)
		{
			throw new CompletionException("Error while failing test event "+eventId, e);
		}
	}
	
	protected void removeMessages(PageId pageId, BookOperators bookOps)
	{
		PageSessionsOperator pageSessionsOp = bookOps.getPageSessionsOperator();
		MessageBatchOperator messageOp = bookOps.getMessageBatchOperator();
		String pageName = pageId.getName();
		
		PagingIterable<PageSessionEntity> rs = pageSessionsOp.get(pageName, readAttrs);
		for (PageSessionEntity session : rs)
			messageOp.remove(session.getPage(), session.getSessionAlias(), session.getDirection(), writeAttrs);
		pageSessionsOp.remove(pageName, writeAttrs);
	}
	
	protected void removeTestEvents(PageId pageId, BookOperators bookOps)
	{
		PageScopesOperator pageScopesOp = bookOps.getPageScopesOperator();
		TestEventOperator eventOp = bookOps.getTestEventOperator();
		String pageName = pageId.getName();
		
		PagingIterable<PageScopeEntity> rs = pageScopesOp.get(pageName, readAttrs);
		for (PageScopeEntity scope : rs)
			eventOp.remove(scope.getPage(), scope.getScope(), writeAttrs);
		pageScopesOp.remove(pageName, writeAttrs);
	}
	
	protected void removePage(PageInfo pageInfo, BookOperators bookOps)
	{
		String book = pageInfo.getId().getBookId().getName();
		bookOps.getPageNameOperator().remove(book, pageInfo.getId().getName(), writeAttrs);
		
		LocalDateTime ldt = TimeUtils.toLocalTimestamp(pageInfo.getStarted());
		bookOps.getPageOperator().remove(book, 
				ldt.toLocalDate(), 
				ldt.toLocalTime(), 
				Instant.now(), 
				writeAttrs);
	}
}

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
import com.exactpro.cradle.*;
import com.exactpro.cradle.cassandra.connection.CassandraConnection;
import com.exactpro.cradle.cassandra.connection.CassandraConnectionSettings;
import com.exactpro.cradle.cassandra.counters.FrameInterval;
import com.exactpro.cradle.cassandra.dao.*;
import com.exactpro.cradle.cassandra.dao.books.*;
import com.exactpro.cradle.cassandra.dao.messages.*;
import com.exactpro.cradle.cassandra.dao.testevents.*;
import com.exactpro.cradle.cassandra.iterators.ConvertingPagedIterator;
import com.exactpro.cradle.cassandra.iterators.PagedIterator;
import com.exactpro.cradle.cassandra.keyspaces.BookKeyspaceCreator;
import com.exactpro.cradle.cassandra.keyspaces.CradleInfoKeyspaceCreator;
import com.exactpro.cradle.cassandra.metrics.DriverMetrics;
import com.exactpro.cradle.cassandra.resultset.CassandraCradleResultSet;
import com.exactpro.cradle.cassandra.resultset.SessionsStatisticsIteratorProvider;
import com.exactpro.cradle.cassandra.retries.FixedNumberRetryPolicy;
import com.exactpro.cradle.cassandra.retries.PageSizeAdjustingPolicy;
import com.exactpro.cradle.cassandra.retries.SelectExecutionPolicy;
import com.exactpro.cradle.cassandra.retries.SelectQueryExecutor;
import com.exactpro.cradle.cassandra.utils.QueryExecutor;
import com.exactpro.cradle.cassandra.utils.StorageUtils;
import com.exactpro.cradle.cassandra.workers.EventsWorker;
import com.exactpro.cradle.cassandra.workers.MessagesWorker;
import com.exactpro.cradle.cassandra.workers.StatisticsWorker;
import com.exactpro.cradle.cassandra.workers.WorkerSupplies;
import com.exactpro.cradle.counters.Counter;
import com.exactpro.cradle.counters.CounterSample;
import com.exactpro.cradle.counters.Interval;
import com.exactpro.cradle.intervals.IntervalsWorker;
import com.exactpro.cradle.messages.*;
import com.exactpro.cradle.resultset.CradleResultSet;
import com.exactpro.cradle.testevents.StoredTestEvent;
import com.exactpro.cradle.testevents.StoredTestEventId;
import com.exactpro.cradle.testevents.TestEventFilter;
import com.exactpro.cradle.testevents.TestEventToStore;
import com.exactpro.cradle.utils.CradleStorageException;
import com.exactpro.cradle.utils.TimeUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

public class CassandraCradleStorage extends CradleStorage
{
	private Logger logger = LoggerFactory.getLogger(CassandraCradleStorage.class);
	
	private final CassandraConnection connection;
	private final CassandraStorageSettings settings;
	
	private CradleOperators ops;
	private QueryExecutor exec;
	private SelectQueryExecutor selectExecutor;
	private Function<BoundStatementBuilder, BoundStatementBuilder> writeAttrs,
			readAttrs,
			strictReadAttrs;
	private SelectExecutionPolicy multiRowResultExecPolicy, singleRowResultExecPolicy;
	private EventsWorker eventsWorker;
	private MessagesWorker messagesWorker;
	private BookCache bookCache;
	private StatisticsWorker statisticsWorker;


	public CassandraCradleStorage(CassandraConnectionSettings connectionSettings, CassandraStorageSettings storageSettings, 
			ExecutorService composingService) throws CradleStorageException
	{
		super(composingService, storageSettings.getMaxMessageBatchSize(),
				storageSettings.getMaxMessageBatchDurationLimit(), storageSettings.getMaxTestEventBatchSize());
		this.connection = new CassandraConnection(connectionSettings, storageSettings.getTimeout());
		this.settings = storageSettings;

		this.multiRowResultExecPolicy = settings.getMultiRowResultExecutionPolicy();
		if (this.multiRowResultExecPolicy == null)
			this.multiRowResultExecPolicy = new PageSizeAdjustingPolicy(settings.getResultPageSize(), 2);

		this.singleRowResultExecPolicy = settings.getSingleRowResultExecutionPolicy();
		if (this.singleRowResultExecPolicy == null)
			this.singleRowResultExecPolicy = new FixedNumberRetryPolicy(5);
	}

	private static final Consumer<Object> NOOP = whatever -> {};

	@Override
	protected BookCache getBookCache() {
		return bookCache;
	}

	@Override
	protected void doInit(boolean prepareStorage) throws CradleStorageException
	{
		connectToCassandra();
		
		try
		{
			DriverMetrics.register(connection.getSession());
			exec = new QueryExecutor(connection.getSession(),
					settings.getTimeout(), settings.getWriteConsistencyLevel(), settings.getReadConsistencyLevel());
			selectExecutor = new SelectQueryExecutor(connection.getSession(), composingService, multiRowResultExecPolicy,
							singleRowResultExecPolicy);
			if (prepareStorage)
				createStorage();
			else
				logger.info("Storage creation skipped");
			

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
			ops = createOperators(connection.getSession(), settings);


			statisticsWorker = new StatisticsWorker(ops, writeAttrs, settings.getCounterPersistanceInterval());
			WorkerSupplies ws = new WorkerSupplies(settings, ops, composingService, bpc, selectExecutor, writeAttrs, readAttrs);
			eventsWorker = new EventsWorker(ws, statisticsWorker);
			messagesWorker = new MessagesWorker(ws, statisticsWorker, statisticsWorker);
			statisticsWorker.start();

			bookCache = new ReadThroughBookCache(ops, readAttrs, settings.getSchemaVersion());
		}
		catch (Exception e)
		{
			throw new CradleStorageException("Could not initialize Cassandra storage", e);
		}
	}
	
	@Override
	protected void doDispose() throws CradleStorageException
	{
		statisticsWorker.stop();
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
	protected Collection<PageInfo> doGetAllPages(BookId bookId) throws CradleStorageException {
			return getBookCache().loadPageInfo(bookId, true);
	}

	@Override
	protected Collection<BookListEntry> doListBooks() {
		return ops.getCradleBookOperator().getAll(readAttrs).all().stream()
				.map(entity -> new BookListEntry(entity.getName(), entity.getSchemaVersion()))
				.collect(Collectors.toList());
	}

	@Override
	protected void doAddBook(BookToAdd newBook, BookId bookId) throws IOException
	{
		BookEntity bookEntity = new BookEntity(newBook, settings.getSchemaVersion());
		if (ops.getCradleBookOperator().get(newBook.getName(), readAttrs) != null) {
			logger.info("Book {} already exists, skipping creation", newBook.getName());
		} else {
			createBookKeyspace(bookEntity);
		}

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

		ops.getCradleBookStatusOp().deleteBookStatuses(bookId.getName());
	}
	
	@Override
	protected void doAddPages(BookId bookId, List<PageInfo> pages, PageInfo lastPage)
			throws CradleStorageException, IOException
	{
		BookOperators bookOps = ops.getOperators(bookId);
		PageOperator pageOp = bookOps.getPageOperator();
		PageNameOperator pageNameOp = bookOps.getPageNameOperator();
		
		String bookName = bookId.getName();
		for (PageInfo page : pages)
		{
			String pageName = page.getId().getName();
			try
			{
				PageNameEntity nameEntity = new PageNameEntity(bookName, pageName, page.getStarted(), page.getComment(), page.getEnded());
				if (!pageNameOp.writeNew(nameEntity, writeAttrs).wasApplied())
					throw new IOException("Query to insert page '"+nameEntity.getName()+"' was not applied. Probably, page already exists");
				PageEntity entity = new PageEntity(bookName, pageName, page.getStarted(), page.getComment(), page.getEnded());
				pageOp.write(entity, writeAttrs);
			}
			catch (IOException e)
			{
				throw e;
			}
			catch (Exception e)
			{
				throw new IOException("Error while writing info of page '"+pageName+"'", e);
			}
		}
		
		if (lastPage != null)
		{
			pageOp.update(new PageEntity(lastPage), writeAttrs);
			pageNameOp.update(new PageNameEntity(lastPage), writeAttrs);
		}
	}
	
	@Override
	protected Collection<PageInfo> doLoadPages(BookId bookId) throws CradleStorageException
	{
		return bookCache.loadPageInfo(bookId, false);
	}
	
	@Override
	protected void doRemovePage(PageInfo page) throws CradleStorageException
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
		PageId pageId = page.getId();
		BookId bookId = pageId.getBookId();

		try
		{
			MessageBatchEntity entity = messagesWorker.createMessageBatchEntity(batch, pageId);
			messagesWorker.storeMessageBatch(entity, bookId).get();
			messagesWorker.storeSession(batch).get();
			messagesWorker.storePageSession(batch, pageId).get();
		}
		catch (Exception e)
		{
			throw new IOException("Error while storing message batch "+batch.getId(), e);
		}
	}

	@Override
	protected void doStoreGroupedMessageBatch(MessageBatchToStore batch, PageInfo page, String groupName)
			throws IOException
	{
		PageId pageId = page.getId();
		BookId bookId = pageId.getBookId();

		try
		{
			GroupedMessageBatchEntity entity = messagesWorker.createGroupedMessageBatchEntity(batch, pageId, groupName);
			messagesWorker.storeMessageBatch(entity.getMessageBatchEntity(), bookId).get();
			messagesWorker.storeGroupedMessageBatch(new GroupedMessageBatchEntity(entity.getMessageBatchEntity(),
					groupName), bookId).get();
			messagesWorker.storeSession(batch).get();
			messagesWorker.storePageSession(batch, pageId).get();
		}
		catch (Exception e)
		{
			throw new IOException("Error while storing message batch "+batch.getId(), e);
		}
	}

	@Override
	protected CompletableFuture<Void> doStoreMessageBatchAsync(MessageBatchToStore batch, PageInfo page)
	{
		PageId pageId = page.getId();
		BookId bookId = pageId.getBookId();

		return CompletableFuture.supplyAsync(() ->
				{
					try
					{
						return messagesWorker.createMessageBatchEntity(batch, pageId);
					}
					catch (IOException e)
					{
						throw new CompletionException(e);
					}
				}, composingService)
				.thenComposeAsync(entity -> messagesWorker.storeMessageBatch(entity, bookId), composingService)
				.thenComposeAsync(r -> messagesWorker.storeSession(batch), composingService)
				.thenComposeAsync(r -> messagesWorker.storePageSession(batch, pageId), composingService)
				.thenAccept(NOOP);
	}

	@Override
	protected CompletableFuture<Void> doStoreGroupedMessageBatchAsync(MessageBatchToStore batch, PageInfo page,
			String groupName) throws IOException, CradleStorageException
	{
		PageId pageId = page.getId();
		BookId bookId = pageId.getBookId();

		return CompletableFuture.supplyAsync(() ->
		{
			try
			{
				return messagesWorker.createGroupedMessageBatchEntity(batch, pageId, groupName);
			}
			catch (IOException e)
			{
				throw new CompletionException(e);
			}
		}, composingService)
				.thenComposeAsync(entity -> messagesWorker.storeGroupedMessageBatch(entity, bookId))
				.thenComposeAsync(entity -> messagesWorker.storeMessageBatch(entity.getMessageBatchEntity(), bookId), composingService)
				.thenComposeAsync(r -> messagesWorker.storeSession(batch), composingService)
				.thenComposeAsync(r -> messagesWorker.storePageSession(batch, pageId), composingService)
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
			TestEventEntity entity = eventsWorker.createEntity(event, pageId);
			eventsWorker.storeEntity(entity, bookId).get();
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
						return eventsWorker.createEntity(event, pageId);
					}
					catch (IOException e)
					{
						throw new CompletionException(e);
					}
				}, composingService)
				.thenComposeAsync(entity -> eventsWorker.storeEntity(entity, bookId), composingService)
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
	{
		return messagesWorker.getMessage(id, pageId);
	}

	@Override
	protected StoredMessageBatch doGetMessageBatch(StoredMessageId id, PageId pageId) throws IOException
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
	protected CompletableFuture<StoredMessageBatch> doGetMessageBatchAsync(StoredMessageId id, PageId pageId)
	{
		return messagesWorker.getMessageBatch(id, pageId);
	}
	
	
	@Override
	protected CradleResultSet<StoredMessage> doGetMessages(MessageFilter filter, BookInfo book) throws IOException
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
	protected CompletableFuture<CradleResultSet<StoredMessage>> doGetMessagesAsync(MessageFilter filter, BookInfo book)
			throws CradleStorageException
	{
		return messagesWorker.getMessages(filter, book);
	}
	
	@Override
	protected CradleResultSet<StoredMessageBatch> doGetMessageBatches(MessageFilter filter, BookInfo book) throws IOException
	{
		try
		{
			return doGetMessageBatchesAsync(filter, book).get();
		}
		catch (Exception e)
		{
			throw new IOException("Error while getting message batches filtered by "+filter, e);
		}
	}

	@Override
	protected CradleResultSet<StoredMessageBatch> doGetGroupedMessageBatches(GroupedMessageFilter filter, BookInfo book)
			throws IOException, CradleStorageException
	{
		try
		{
			return doGetGroupedMessageBatchesAsync(filter, book).get();
		}
		catch (Exception e)
		{
			throw new IOException("Error while getting grouped message batches filtered by "+filter, e);
		}
	}

	@Override
	protected CompletableFuture<CradleResultSet<StoredMessageBatch>> doGetMessageBatchesAsync(MessageFilter filter, BookInfo book)
			throws CradleStorageException
	{
		return messagesWorker.getMessageBatches(filter, book);
	}

	@Override
	protected CompletableFuture<CradleResultSet<StoredMessageBatch>> doGetGroupedMessageBatchesAsync(
			GroupedMessageFilter filter, BookInfo book) throws CradleStorageException
	{
		return messagesWorker.getGroupedMessageBatches(filter, book, getSettings().getMaxMessageBatchDurationLimit());
	}

	@Override
	protected long doGetLastSequence(String sessionAlias, Direction direction, BookId bookId)
			throws CradleStorageException
	{
		return messagesWorker.getBoundarySequence(sessionAlias, direction, bpc.getBook(bookId), false);
	}

	@Override
	protected long doGetFirstSequence(String sessionAlias, Direction direction, BookId bookId)
			throws CradleStorageException
	{
		return messagesWorker.getBoundarySequence(sessionAlias, direction, bpc.getBook(bookId), true);
	}

	@Override
	protected Collection<String> doGetSessionAliases(BookId bookId) throws IOException, CradleStorageException
	{
		MappedAsyncPagingIterable<SessionEntity> entities;
		String queryInfo = String.format("Getting session aliases for book '%s'", bookId);
		BookOperators bookOps = null;
		try
		{
			bookOps = ops.getOperators(bookId);
			CompletableFuture<MappedAsyncPagingIterable<SessionEntity>> future =
					bookOps.getSessionsOperator().get(bookId.getName(), readAttrs);
			entities = selectExecutor.executeMappedMultiRowResultQuery(
					() -> future, bookOps.getSessionEntityConverter()::getEntity, queryInfo).get();
		}
		catch (Exception e)
		{
			throw new CradleStorageException("Error occurred while " + StringUtils.uncapitalize(queryInfo), e);
		}
		
		if (entities == null)
			return Collections.emptySet();
		
		Collection<String> result = new HashSet<>();
		PagedIterator<SessionEntity> it = new PagedIterator<>(entities, selectExecutor,
				bookOps.getSessionEntityConverter()::getEntity, "Fetching next page with session aliases");
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
			throws CradleStorageException
	{
		return eventsWorker.getTestEvents(filter, book);
	}
	
	
	@Override
	protected Collection<String> doGetScopes(BookId bookId) throws IOException, CradleStorageException
	{
		MappedAsyncPagingIterable<ScopeEntity> entities;
		String queryInfo = String.format("get scopes for book '%s'", bookId);
		BookOperators bookOps = null;
		try
		{
			bookOps = ops.getOperators(bookId);
			CompletableFuture<MappedAsyncPagingIterable<ScopeEntity>> future =
					bookOps.getScopeOperator().get(bookId.getName(), readAttrs);
			entities = selectExecutor.executeMappedMultiRowResultQuery(() -> future,
					bookOps.getScopeEntityConverter()::getEntity, queryInfo).get();
		}
		catch (Exception e)
		{
			throw new IOException("Error while " + queryInfo, e);
		}
		
		if (entities == null)
			return Collections.emptySet();
		
		Collection<String> result = new HashSet<>();
		PagedIterator<ScopeEntity> it = new PagedIterator<>(entities, selectExecutor,
				bookOps.getScopeEntityConverter()::getEntity, "Fetching next page of scopes");
		while (it.hasNext())
			result.add(it.next().getScope());
		return result;
	}

	@Override
	protected CompletableFuture<CradleResultSet<CounterSample>> doGetMessageCountersAsync(BookId bookId,
																						  String sessionAlias,
																						  Direction direction,
																						  FrameType frameType,
																						  Interval interval) throws CradleStorageException {
		String queryInfo = String.format("Counters for Messages with sessionAlias-%s, direction-%s, frameType-%s from %s to %s",
				sessionAlias,
				direction.name(),
				frameType.name(),
				interval.getStart().toString(),
				interval.getEnd().toString());

		logger.info("Getting {}", queryInfo);
		BookOperators operators = ops.getOperators(bookId);
		MessageStatisticsIteratorProvider iteratorProvider = new MessageStatisticsIteratorProvider(queryInfo,
				operators,
				bpc.getBook(bookId),
				composingService,
				selectExecutor,
				sessionAlias,
				direction,
				new FrameInterval(frameType, interval),
				readAttrs);

		return iteratorProvider.nextIterator()
				.thenApplyAsync(r -> new CassandraCradleResultSet<>(r, iteratorProvider), composingService);
	}



	@Override
	protected CradleResultSet<CounterSample> doGetMessageCounters(BookId bookId,
																  String sessionAlias,
																  Direction direction,
																  FrameType frameType,
																  Interval interval) throws IOException {
		String queryInfo = String.format("Counters for Messages with sessionAlias-%s, direction-%s, frameType-%s from %s to %s",
				sessionAlias,
				direction.name(),
				frameType.name(),
				interval.getStart().toString(),
				interval.getEnd().toString());
		try
		{
			return doGetMessageCountersAsync(bookId, sessionAlias, direction, frameType, interval).get();
		}
		catch (Exception e)
		{
			throw new IOException("Error while getting " + queryInfo, e);
		}
	}

	@Override
	protected CompletableFuture<CradleResultSet<CounterSample>> doGetCountersAsync(BookId bookId,
																				   EntityType entityType,
																				   FrameType frameType,
																				   Interval interval) throws CradleStorageException {
		String queryInfo = String.format("Counters for %s with frameType-%s from %s to %s",
				entityType.name(),
				frameType.name(),
				interval.getStart().toString(),
				interval.getEnd().toString());

		logger.info("Getting {}", queryInfo);

		BookOperators operators = ops.getOperators(bookId);

		EntityStatisticsIteratorProvider iteratorProvider = new EntityStatisticsIteratorProvider(queryInfo,
						operators,
						refreshBook(bookId.getName()),
						composingService,
						selectExecutor,
						entityType,
						frameType,
						new FrameInterval(frameType, interval),
						readAttrs);

		return iteratorProvider.nextIterator()
				.thenApplyAsync(r -> new CassandraCradleResultSet<>(r, iteratorProvider), composingService);
	}

	@Override
	protected CradleResultSet<CounterSample> doGetCounters(BookId bookId,
														   EntityType entityType,
														   FrameType frameType,
														   Interval interval) throws CradleStorageException, IOException {
		String queryInfo = String.format("Counters for %s with frameType-%s from %s to %s",
				entityType.name(),
				frameType.name(),
				interval.getStart().toString(),
				interval.getEnd().toString());
		try
		{
			return doGetCountersAsync(bookId, entityType, frameType, interval).get();
		}
		catch (Exception e)
		{
			throw new IOException("Error while getting " + queryInfo, e);
		}
	}

	@Override
	protected CompletableFuture<Counter> doGetMessageCountAsync(BookId bookId,
																String sessionAlias,
																Direction direction,
																Interval interval) throws CradleStorageException {
		String queryInfo = String.format("Cumulative count for Messages with session_alias-%s, direction-%s from %s to %s",
				sessionAlias,
				direction,
				interval.getStart().toString(),
				interval.getEnd().toString());

		logger.info("Getting {}", queryInfo);

		List<FrameInterval> slices = StorageUtils.sliceInterval(interval);

		// Accumulate counters
		return CompletableFuture.supplyAsync(() -> {
			Counter sum = new Counter(0, 0);
			for (var el : slices) {
				try {
					CradleResultSet<CounterSample> res = getMessageCounters(bookId,
							sessionAlias,
							direction,
							el.getFrameType(),
							el.getInterval());
					while (res.hasNext()) {
						sum = sum.incrementedBy(res.next().getCounter());
					}
				} catch (CradleStorageException | IOException e) {
					logger.error("Error while getting {}, cause - {}", queryInfo, e.getCause());
				}
			}

			return sum;
		}, composingService);
	}

	@Override
	protected Counter doGetMessageCount(BookId bookId, String sessionAlias, Direction direction, Interval interval) throws CradleStorageException, IOException {
		String queryInfo = String.format("Cumulative count for Messages with session_alias-%s, direction-%s from %s to %s",
				sessionAlias,
				direction,
				interval.getStart().toString(),
				interval.getEnd().toString());
		try
		{
			return doGetMessageCountAsync(bookId, sessionAlias, direction, interval).get();
		}
		catch (Exception e)
		{
			throw new IOException("Error while getting " + queryInfo, e);
		}
	}

	@Override
	protected CompletableFuture<Counter> doGetCountAsync(BookId bookId, EntityType entityType, Interval interval) throws CradleStorageException {
		String queryInfo = String.format("Cumulative count for %s with from %s to %s",
				entityType.name(),
				interval.getStart().toString(),
				interval.getEnd().toString());

		logger.info("Getting {}", queryInfo);

		List<FrameInterval> slices = StorageUtils.sliceInterval(interval);

		// Accumulate counters
		return CompletableFuture.supplyAsync(() -> {
			Counter sum = new Counter(0, 0);
			for (var el : slices) {
				try {
					CradleResultSet<CounterSample> res = getCounters(bookId,
							entityType,
							el.getFrameType(),
							el.getInterval());

					while (res.hasNext()) {
						sum = sum.incrementedBy(res.next().getCounter());
					}
				} catch (CradleStorageException | IOException e) {
					logger.error("Error while getting {}, cause - {}", queryInfo, e.getCause());
				}
			}

			return sum;
		}, composingService);
	}

	@Override
	protected Counter doGetCount(BookId bookId, EntityType entityType, Interval interval) throws CradleStorageException, IOException {
		String queryInfo = String.format("Cumulative count for %s with from %s to %s",
				entityType.name(),
				interval.getStart().toString(),
				interval.getEnd().toString());
		try
		{
			return doGetCountAsync(bookId, entityType, interval).get();
		}
		catch (Exception e)
		{
			throw new IOException("Error while getting " + queryInfo, e);
		}
	}

	private CompletableFuture<CradleResultSet<String >> doGetSessionsAsync (BookId bookId, Interval interval, SessionRecordType recordType) throws CradleStorageException {
		String queryInfo = String.format("%s Aliases in book %s from %s to %s",
				recordType.name(),
				bookId.getName(),
				interval.getStart().toString(),
				interval.getEnd().toString());

		List<FrameInterval> frameIntervals = StorageUtils.sliceInterval(interval);

		SessionsStatisticsIteratorProvider iteratorProvider = new SessionsStatisticsIteratorProvider(
				queryInfo,
				ops.getOperators(bookId),
				bpc.getBook(bookId),
				composingService,
				selectExecutor,
				readAttrs,
				frameIntervals,
				recordType);

		return iteratorProvider.nextIterator()
				.thenApplyAsync(it -> new CassandraCradleResultSet<>(it, iteratorProvider));
	}

	@Override
	protected CompletableFuture<CradleResultSet<String>> doGetSessionAliasesAsync(BookId bookId, Interval interval) throws CradleStorageException {
		return doGetSessionsAsync(bookId, interval, SessionRecordType.SESSION);
	}

	@Override
	protected CradleResultSet<String> doGetSessionAliases(BookId bookId, Interval interval) throws CradleStorageException {
		try
		{
			return doGetSessionAliasesAsync(bookId, interval).get();
		}
		catch (Exception e)
		{
			throw new CradleStorageException("Error while getting Session Aliases", e);
		}
	}

	@Override
	protected CradleResultSet<String> doGetSessionGroups(BookId bookId, Interval interval) throws CradleStorageException {
		try
		{
			return doGetSessionGroupsAsync(bookId, interval).get();
		}
		catch (Exception e)
		{
			throw new CradleStorageException("Error while getting Session Groups", e);
		}
	}

	@Override
	protected CompletableFuture<CradleResultSet<String>> doGetSessionGroupsAsync(BookId bookId, Interval interval) throws CradleStorageException {
		return doGetSessionsAsync(bookId, interval, SessionRecordType.SESSION_GROUP);
	}

	@Override
	protected PageInfo doUpdatePageComment(BookId bookId, String pageName, String comment) throws CradleStorageException {
		PageOperator pageOperator = ops.getOperators(bookId).getPageOperator();
		PageNameOperator pageNameOperator = ops.getOperators(bookId).getPageNameOperator();

		PageNameEntity pageNameEntity = pageNameOperator.get(bookId.getName(), pageName).one();
		if (pageNameEntity == null)
			throw new CradleStorageException(String.format("Page \"%s\" not found in book \"%s\"", pageName, bookId.getName()));

		PageEntity pageEntity = pageOperator.get(bookId.getName(),
					pageNameEntity.getStartDate(),
					pageNameEntity.getStartTime().minusNanos(1),
					readAttrs).one();

		if (pageEntity == null || !pageEntity.getName().equals(pageNameEntity.getName()))
			throw new CradleStorageException(String.format("Inconsistent data for page \"%s\" in book %s", pageName, bookId.getName()));

		pageNameEntity.setComment(comment);
		pageEntity.setComment(comment);
		try {
			pageNameOperator.update(pageNameEntity, readAttrs);
			pageOperator.update(pageEntity, readAttrs);
		} catch (Exception e) {
			throw new CradleStorageException(String.format("Failed to update page comment, this might result in broken state, try again. %s", e.getCause()));
		}

		return pageEntity.toPageInfo();
	}

	@Override
	protected PageInfo doUpdatePageName(BookId bookId, String oldPageName, String newPageName) throws CradleStorageException {
		PageOperator pageOperator = ops.getOperators(bookId).getPageOperator();
		PageNameOperator pageNameOperator = ops.getOperators(bookId).getPageNameOperator();

		PageNameEntity pageNameEntity = pageNameOperator.get(bookId.getName(), oldPageName).one();
		if (pageNameEntity == null)
			throw new CradleStorageException(String.format("Page \"%s\" not found in book \"%s\"", oldPageName, bookId.getName()));

		PageEntity pageEntity = pageOperator.get(bookId.getName(),
				pageNameEntity.getStartDate(),
				pageNameEntity.getStartTime().minusNanos(1),
				readAttrs).one();

		if (pageEntity == null || !pageEntity.getName().equals(pageNameEntity.getName()))
			throw new CradleStorageException(String.format("Inconsistent data for page \"%s\" in book %s", oldPageName, bookId.getName()));

		pageEntity.setName(newPageName);
		pageNameEntity.setName(newPageName);
		pageNameOperator.remove(bookId.getName(), oldPageName, readAttrs);

		try {
			pageNameOperator.writeNew(pageNameEntity, readAttrs);
			pageOperator.update(pageEntity, readAttrs);
		} catch (Exception e) {
			throw new CradleStorageException(String.format("Failed to update page name, this might result in broken state, try again. %s", e.getCause()));
		}

		return pageEntity.toPageInfo();
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
		return new CradleOperators(dataMapper, settings, readAttrs);
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
//			new BookKeyspaceCreator(bookEntity.getKeyspaceName(), exec, settings).createAll();
			new BookKeyspaceCreator(bookEntity, exec, settings, ops.getCradleBookStatusOp()).createAll();
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

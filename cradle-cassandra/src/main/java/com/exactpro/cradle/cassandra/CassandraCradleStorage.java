/*
 * Copyright 2020-2023 Exactpro (Exactpro Systems Limited)
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
import com.datastax.oss.driver.api.core.cql.BatchStatementBuilder;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.exactpro.cradle.*;
import com.exactpro.cradle.cassandra.connection.CassandraConnection;
import com.exactpro.cradle.cassandra.connection.CassandraConnectionSettings;
import com.exactpro.cradle.cassandra.counters.FrameInterval;
import com.exactpro.cradle.cassandra.dao.*;
import com.exactpro.cradle.cassandra.dao.books.*;
import com.exactpro.cradle.cassandra.dao.intervals.CassandraIntervalsWorker;
import com.exactpro.cradle.cassandra.dao.messages.*;
import com.exactpro.cradle.cassandra.dao.statistics.EntityStatisticsIteratorProvider;
import com.exactpro.cradle.cassandra.dao.statistics.MessageStatisticsIteratorProvider;
import com.exactpro.cradle.cassandra.dao.statistics.MessageStatisticsOperator;
import com.exactpro.cradle.cassandra.dao.statistics.SessionStatisticsOperator;
import com.exactpro.cradle.cassandra.dao.testevents.*;
import com.exactpro.cradle.cassandra.iterators.ConvertingPagedIterator;
import com.exactpro.cradle.cassandra.iterators.PagedIterator;
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
import com.exactpro.cradle.iterators.ConvertingIterator;
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
import java.time.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
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
	
	private CassandraOperators operators;
	private QueryExecutor exec;
	private SelectQueryExecutor selectExecutor;
	private Function<BoundStatementBuilder, BoundStatementBuilder> writeAttrs,
			readAttrs,
			strictReadAttrs;
	private Function<BatchStatementBuilder, BatchStatementBuilder> batchWriteAttrs;
	private SelectExecutionPolicy multiRowResultExecPolicy, singleRowResultExecPolicy;
	private EventsWorker eventsWorker;
	private MessagesWorker messagesWorker;
	private BookCache bookCache;
	private StatisticsWorker statisticsWorker;
	private EventBatchDurationWorker eventBatchDurationWorker;
	private IntervalsWorker intervalsWorker;


	public CassandraCradleStorage(CassandraConnectionSettings connectionSettings, CassandraStorageSettings storageSettings, 
			ExecutorService composingService) throws CradleStorageException
	{
		super(composingService, storageSettings.getMaxMessageBatchSize(), storageSettings.getMaxTestEventBatchSize());
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
			batchWriteAttrs = builder -> builder.setConsistencyLevel(settings.getWriteConsistencyLevel())
					.setTimeout(timeout);
			readAttrs = builder -> builder.setConsistencyLevel(settings.getReadConsistencyLevel())
					.setTimeout(timeout)
					.setPageSize(resultPageSize);
			strictReadAttrs = builder -> builder.setConsistencyLevel(ConsistencyLevel.ALL)
					.setTimeout(timeout)
					.setPageSize(resultPageSize);
			operators = createOperators(connection.getSession(), settings);
			bookCache = new ReadThroughBookCache(operators, readAttrs, settings.getSchemaVersion());
			bookManager = new BookManager(getBookCache(), settings.getBookRefreshIntervalMillis());
			bookManager.start();

			eventBatchDurationWorker = new EventBatchDurationWorker(
					new EventBatchDurationCache(settings.getEventBatchDurationCacheSize()),
					operators.getEventBatchMaxDurationOperator(),
					settings.getEventBatchDurationMillis());

			WorkerSupplies ws = new WorkerSupplies(settings, operators, composingService, bookCache, selectExecutor, writeAttrs, readAttrs, batchWriteAttrs);
			statisticsWorker = new StatisticsWorker(ws, settings.getCounterPersistenceInterval());
			eventsWorker = new EventsWorker(ws, statisticsWorker, eventBatchDurationWorker);
			messagesWorker = new MessagesWorker(ws, statisticsWorker, statisticsWorker);
			statisticsWorker.start();
			intervalsWorker = new CassandraIntervalsWorker(ws);
		}
		catch (Exception e)
		{
			throw new CradleStorageException("Could not initialize Cassandra storage", e);
		}
	}
	
	@Override
	protected void doDispose() throws CradleStorageException
	{
		bookManager.stop();
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
		return operators.getBookOperator().getAll(readAttrs).all().stream()
				.map(entity -> new BookListEntry(entity.getName(), entity.getSchemaVersion()))
				.collect(Collectors.toList());
	}

	@Override
	protected void doAddBook(BookToAdd book, BookId bookId) throws IOException
	{

		BookEntity entity = new BookEntity(book.getName(), book.getFullName(), book.getDesc(), book.getCreated(), settings.getSchemaVersion());
		try
		{
			if (!operators.getBookOperator().write(entity, writeAttrs).wasApplied())
				throw new IOException("Query to insert book '"+entity.getName()+"' was not applied. Probably, book already exists");
		}
		catch (IOException e)
		{
			throw e;
		}
		catch (Exception e)
		{
			throw new IOException("Error while writing info of book '"+bookId+"'", e);
		}
	}
	
	@Override
	protected void doAddPages(BookId bookId, List<PageInfo> pages, PageInfo lastPage)
			throws CradleStorageException, IOException
	{
		PageOperator pageOp = operators.getPageOperator();
		PageNameOperator pageNameOp = operators.getPageNameOperator();
		
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

		removeSessionData(pageId);
		removeGroupData(pageId);
		removeTestEventData(pageId);
		removePageDurations(pageId);
		removeEntityStatistics(pageId);
		removePageData(page);
	}

	private void removePageDurations (PageId pageId) {
		eventBatchDurationWorker.removePageDurations(pageId);
	}
	
	@Override
	protected void doStoreMessageBatch(MessageBatchToStore batch, PageInfo page) throws IOException {
		PageId pageId = page.getId();
		try {
			messagesWorker.storeMessageBatch(batch, pageId).get();
			messagesWorker.storeSession(batch).get();
			messagesWorker.storePageSession(batch, pageId).get();
		} catch (Exception e) {
			throw new IOException("Exception while storing message batch " + batch.getId(), e);
		}
	}

	@Override
	protected void doStoreGroupedMessageBatch(GroupedMessageBatchToStore batch, PageInfo page)
			throws IOException
	{
		PageId pageId = page.getId();

		try
		{
			messagesWorker.storeGroupedMessageBatch(batch, pageId).get();

			for (MessageBatchToStore b: batch.getSessionMessageBatches()) {
				messagesWorker.storeMessageBatch(b, pageId).get();
				messagesWorker.storeSession(b).get();
				messagesWorker.storePageSession(b, pageId).get();
			}
		}
		catch (Exception e)
		{
			throw new IOException("Error while storing message batch "+batch.getBookId(), e);
		}
	}

	@Override
	protected CompletableFuture<Void> doStoreMessageBatchAsync(MessageBatchToStore batch, PageInfo page) {
		PageId pageId = page.getId();
		return messagesWorker.storeMessageBatch(batch, pageId)
				.thenComposeAsync((unused) -> messagesWorker.storeSession(batch), composingService)
				.thenComposeAsync((unused) -> messagesWorker.storePageSession(batch, pageId), composingService)
				.thenAccept(NOOP);
	}

	@Override
	protected CompletableFuture<Void> doStoreGroupedMessageBatchAsync(GroupedMessageBatchToStore batch, PageInfo page)
			throws CradleStorageException
	{
		PageId pageId = page.getId();

		CompletableFuture<Void> future =  messagesWorker.storeGroupedMessageBatch(batch, pageId);

		// store individual session message batches
		for (MessageBatchToStore b: batch.getSessionMessageBatches()) {
			future = future.thenComposeAsync((unused) -> messagesWorker.storeMessageBatch(b, pageId), composingService)
							.thenComposeAsync((unused) -> messagesWorker.storeSession(b), composingService)
							.thenComposeAsync((unused) -> messagesWorker.storePageSession(b, pageId), composingService)
							.thenAccept(NOOP);
		}
		return future;
	}

	@Override
	protected void doStoreTestEvent(TestEventToStore event, PageInfo page) throws IOException {
		PageId pageId = page.getId();
		try
		{
			eventsWorker.storeEvent(event, pageId);
			eventsWorker.storeScope(event).get();
			eventsWorker.storePageScope(event, pageId).get();
		}
		catch (Exception e)
		{
			throw new IOException("Error while storing test event " + event.getId(), e);
		}
	}

	@Override
	protected CompletableFuture<Void> doStoreTestEventAsync(TestEventToStore event, PageInfo page) throws IOException, CradleStorageException {
		PageId pageId = page.getId();

		return eventsWorker.storeEvent(event, pageId)
				.thenComposeAsync((unused) -> eventsWorker.storeScope(event), composingService)
				.thenComposeAsync((unused) -> eventsWorker.storePageScope(event, pageId), composingService)
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
	protected CradleResultSet<StoredGroupedMessageBatch> doGetGroupedMessageBatches(GroupedMessageFilter filter, BookInfo book)
			throws IOException
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
	protected CompletableFuture<CradleResultSet<StoredGroupedMessageBatch>> doGetGroupedMessageBatchesAsync(
			GroupedMessageFilter filter, BookInfo book) throws CradleStorageException
	{
		return messagesWorker.getGroupedMessageBatches(filter, book);
	}

	@Override
	protected long doGetLastSequence(String sessionAlias, Direction direction, BookId bookId)
			throws CradleStorageException
	{
		return messagesWorker.getBoundarySequence(sessionAlias, direction, getBookCache().getBook(bookId), false);
	}

	@Override
	protected long doGetFirstSequence(String sessionAlias, Direction direction, BookId bookId)
			throws CradleStorageException
	{
		return messagesWorker.getBoundarySequence(sessionAlias, direction, getBookCache().getBook(bookId), true);
	}

	@Override
	protected Collection<String> doGetSessionAliases(BookId bookId) throws IOException, CradleStorageException
	{
		MappedAsyncPagingIterable<SessionEntity> entities;
		String queryInfo = String.format("Getting session aliases for book '%s'", bookId);
		try
		{
			CompletableFuture<MappedAsyncPagingIterable<SessionEntity>> future =
					operators.getSessionsOperator().get(bookId.getName(), readAttrs);
			entities = selectExecutor.executeMappedMultiRowResultQuery(
					() -> future, operators.getSessionEntityConverter()::getEntity, queryInfo).get();
		}
		catch (Exception e)
		{
			throw new CradleStorageException("Error occurred while " + StringUtils.uncapitalize(queryInfo), e);
		}
		
		if (entities == null)
			return Collections.emptySet();
		
		Collection<String> result = new HashSet<>();
		PagedIterator<SessionEntity> it = new PagedIterator<>(entities, selectExecutor,
				operators.getSessionEntityConverter()::getEntity, "Fetching next page with session aliases");
		while (it.hasNext())
			result.add(it.next().getSessionAlias());
		return result;
	}

	@Override
	protected Collection<String> doGetGroups(BookId bookId) throws IOException, CradleStorageException {

		String queryInfo = String.format("Getting groups for book '%s'", bookId);

		MappedAsyncPagingIterable<GroupEntity> entities = null;
		try {
			var future = operators.getGroupsOperator().get(bookId.getName(), getReadAttrs());

			entities = selectExecutor.executeMappedMultiRowResultQuery(
					() -> future, operators.getGroupEntityConverter()::getEntity, queryInfo).get();
		} catch (InterruptedException | ExecutionException e) {
			throw new CradleStorageException("Error occurred while " + StringUtils.uncapitalize(queryInfo), e);
		}

		if (entities == null) {
			return Collections.emptyList();
		}

		List<String> result = new ArrayList<>();
		PagedIterator<GroupEntity> it = new PagedIterator<>(entities, selectExecutor,
				operators.getGroupEntityConverter()::getEntity, "Fetching next page with groups");

		while (it.hasNext()) {
			result.add(it.next().getGroup());
		}

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
		try
		{
			CompletableFuture<MappedAsyncPagingIterable<ScopeEntity>> future =
					operators.getScopeOperator().get(bookId.getName(), readAttrs);
			entities = selectExecutor.executeMappedMultiRowResultQuery(() -> future,
					operators.getScopeEntityConverter()::getEntity, queryInfo).get();
		}
		catch (Exception e)
		{
			throw new IOException("Error while " + queryInfo, e);
		}
		
		if (entities == null)
			return Collections.emptySet();
		
		Collection<String> result = new HashSet<>();
		PagedIterator<ScopeEntity> it = new PagedIterator<>(entities, selectExecutor,
				operators.getScopeEntityConverter()::getEntity, "Fetching next page of scopes");
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
		MessageStatisticsIteratorProvider iteratorProvider = new MessageStatisticsIteratorProvider(queryInfo,
				operators,
				getBookCache().getBook(bookId),
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
				operators,
				getBookCache().getBook(bookId),
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
		PageOperator pageOperator = operators.getPageOperator();
		PageNameOperator pageNameOperator = operators.getPageNameOperator();

		PageNameEntity pageNameEntity = pageNameOperator.get(bookId.getName(), pageName).one();
		if (pageNameEntity == null)
			throw new CradleStorageException(String.format("Page \"%s\" not found in book \"%s\"", pageName, bookId.getName()));

		PageEntity pageEntity = pageOperator.get(bookId.getName(),
					pageNameEntity.getStartDate(),
					pageNameEntity.getStartTime().minusNanos(1),
					readAttrs).one();

		if (pageEntity == null || !pageEntity.getName().equals(pageNameEntity.getName()))
			throw new CradleStorageException(String.format("Inconsistent data for page \"%s\" in book %s", pageName, bookId.getName()));

		PageEntity updatedPageEntity = new PageEntity(pageEntity.getBook(),
				pageEntity.getStartDate(),
				pageEntity.getStartTime(),
				pageEntity.getName(),
				comment,
				pageEntity.getEndDate(),
				pageEntity.getEndTime(),
				Instant.now(),
				pageEntity.getRemoved());

		PageNameEntity updatedPageNameEntity = new PageNameEntity(pageNameEntity.getBook(),
				pageNameEntity.getName(),
				pageNameEntity.getStartDate(),
				pageNameEntity.getStartTime(),
				comment,
				pageNameEntity.getEndDate(),
				pageNameEntity.getEndTime());

		try {
			pageNameOperator.update(updatedPageNameEntity, readAttrs);
			pageOperator.update(updatedPageEntity, readAttrs);
		} catch (Exception e) {
			throw new CradleStorageException(String.format("Failed to update page comment, this might result in broken state, try again. %s", e.getCause()));
		}

		return pageEntity.toPageInfo();
	}

	@Override
	protected PageInfo doUpdatePageName(BookId bookId, String oldPageName, String newPageName) throws CradleStorageException {
		PageOperator pageOperator = operators.getPageOperator();
		PageNameOperator pageNameOperator = operators.getPageNameOperator();

		PageNameEntity pageNameEntity = pageNameOperator.get(bookId.getName(), oldPageName).one();
		if (pageNameEntity == null)
			throw new CradleStorageException(String.format("Page \"%s\" not found in book \"%s\"", oldPageName, bookId.getName()));

		PageEntity pageEntity = pageOperator.get(bookId.getName(),
				pageNameEntity.getStartDate(),
				pageNameEntity.getStartTime().minusNanos(1),
				readAttrs).one();

		if (pageEntity == null || !pageEntity.getName().equals(pageNameEntity.getName()))
			throw new CradleStorageException(String.format("Inconsistent data for page \"%s\" in book %s", oldPageName, bookId.getName()));

		PageInfo pageInfo = pageEntity.toPageInfo();
		Instant now = Instant.now();
		if (pageInfo.getStarted().isBefore(now)) {
			throw new CradleStorageException(
					String.format("You can only rename pages which start in future: pageStart - %s, now - %s",
							pageInfo.getStarted(),
							now));
		}

		PageEntity updatedPageEntity = new PageEntity(pageEntity.getBook(),
				pageEntity.getStartDate(),
				pageEntity.getStartTime(),
				newPageName,
				pageEntity.getComment(),
				pageEntity.getEndDate(),
				pageEntity.getEndTime(),
				Instant.now(),
				pageEntity.getRemoved());

		PageNameEntity newPageNameEntity = new PageNameEntity(pageNameEntity.getBook(),
				newPageName,
				pageNameEntity.getStartDate(),
				pageNameEntity.getStartTime(),
				pageNameEntity.getComment(),
				pageNameEntity.getEndDate(),
				pageNameEntity.getEndTime());

		pageNameOperator.remove(bookId.getName(), oldPageName, readAttrs);

		try {
			pageNameOperator.writeNew(newPageNameEntity, readAttrs);
			pageOperator.update(updatedPageEntity, readAttrs);
		} catch (Exception e) {
			throw new CradleStorageException(String.format("Failed to update page name, this might result in broken state, try again. %s", e.getCause()));
		}

		return pageEntity.toPageInfo();
	}

	@Override
	public IntervalsWorker getIntervalsWorker()
	{
		return intervalsWorker;
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
	
	protected CassandraOperators createOperators(CqlSession session, CassandraStorageSettings settings)
	{
		CassandraDataMapper dataMapper = new CassandraDataMapperBuilder(session).build();
		return new CassandraOperators(dataMapper, settings);
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

	public Function<BatchStatementBuilder, BatchStatementBuilder> getBatchWriteAttrs()
	{
		return batchWriteAttrs;
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
	
	protected void removeSessionData(PageId pageId) {
		String book = pageId.getBookId().getName();
		String page	= pageId.getName();

		PageSessionsOperator pageSessionsOp = operators.getPageSessionsOperator();
		MessageBatchOperator messageOp = operators.getMessageBatchOperator();
		MessageStatisticsOperator messageStatisticsOp = operators.getMessageStatisticsOperator();
		SessionStatisticsOperator sessionStatisticsOp = operators.getSessionStatisticsOperator();
		SessionsOperator sessionsOp = operators.getSessionsOperator();

		PagingIterable<PageSessionEntity> rs = pageSessionsOp.get(book, page, readAttrs);
		for (PageSessionEntity session : rs) {
			messageOp.remove(book, session.getPage(), session.getSessionAlias(), session.getDirection(), writeAttrs);
			sessionsOp.remove(book, session.getSessionAlias(), writeAttrs);

			// remove statistics associated with session
			for (FrameType ft : FrameType.values()) {

				messageStatisticsOp.remove(book, page, session.getSessionAlias(),
						session.getDirection(), ft.getValue(), writeAttrs);

				sessionStatisticsOp.remove(book, page, SessionRecordType.SESSION.getValue(), ft.getValue(),
						writeAttrs);
			}
		}

		pageSessionsOp.remove(book, page, writeAttrs);
	}

	protected void removeGroupData (PageId pageId) {
		String book = pageId.getBookId().getName();
		String page	= pageId.getName();

		GroupsOperator groupsOperator = operators.getGroupsOperator();
		PageGroupsOperator pageGroupsOperator = operators.getPageGroupsOperator();
		GroupedMessageBatchOperator groupMessageOp = operators.getGroupedMessageBatchOperator();
		SessionStatisticsOperator sessionStatisticsOp = operators.getSessionStatisticsOperator();
		MessageStatisticsOperator messageStatisticsOp = operators.getMessageStatisticsOperator();

		PagingIterable<PageGroupEntity> entities = pageGroupsOperator.get(book, page, readAttrs);
		for (PageGroupEntity entity : entities) {
			groupMessageOp.remove(book, page, entity.getGroup(), writeAttrs);
			groupsOperator.remove(book, entity.getGroup(), writeAttrs);

			// Remove group statistics
			for (FrameType ft : FrameType.values()) {
				messageStatisticsOp.remove(
						book,
						page,
						entity.getGroup(),
						"",
						ft.getValue(),
						writeAttrs);

				sessionStatisticsOp.remove(
						book,
						page,
						SessionRecordType.SESSION_GROUP.getValue(),
						ft.getValue(),
						writeAttrs);
			}
		}

		pageGroupsOperator.remove(book, page, writeAttrs);
	}
	
	protected void removeTestEventData(PageId pageId) {
		String book = pageId.getBookId().getName();
		String page = pageId.getName();

		PageScopesOperator pageScopesOp = operators.getPageScopesOperator();
		TestEventOperator eventOp = operators.getTestEventOperator();

		PagingIterable<PageScopeEntity> rs = pageScopesOp.get(book, page, readAttrs);
		for (PageScopeEntity scope : rs)
			eventOp.remove(book, page, scope.getScope(), writeAttrs);

		pageScopesOp.remove(book, page, writeAttrs);
	}
	
	protected void removePageData(PageInfo pageInfo) {

		String book = pageInfo.getId().getBookId().getName();
		String page = pageInfo.getId().getName();

		// remove sessions
		operators.getPageSessionsOperator().remove(book, page, writeAttrs);

		//remove page name
		operators.getPageNameOperator().remove(book, page, writeAttrs);

		//remove page
		LocalDateTime ldt = TimeUtils.toLocalTimestamp(pageInfo.getStarted());
		operators.getPageOperator().remove(book,
				ldt.toLocalDate(), 
				ldt.toLocalTime(), 
				Instant.now(), 
				writeAttrs);
	}

	private void removeEntityStatistics(PageId pageId) {
		String book = pageId.getBookId().getName();
		for (FrameType ft : FrameType.values()) {
			operators.getEntityStatisticsOperator().remove(book, pageId.getName(), EntityType.MESSAGE.getValue(),
					ft.getValue(), writeAttrs);
			operators.getEntityStatisticsOperator().remove(book, pageId.getName(), EntityType.EVENT.getValue(),
					ft.getValue(), writeAttrs);
		}
	}

	@Override
	protected CompletableFuture<Iterator<PageInfo>> doGetPagesAsync (BookId bookId, Interval interval) {
		String queryInfo = String.format(
				"Getting pages for book %s between  %s - %s ",
				bookId.getName(),
				interval.getStart(),
				interval.getEnd());

		PageEntity startPage = operators.getPageOperator()
				.getPageForLessOrEqual(
						bookId.getName(),
						LocalDate.ofInstant(interval.getStart(), TIMEZONE_OFFSET),
						LocalTime.ofInstant(interval.getStart(), TIMEZONE_OFFSET),
						readAttrs).one();

		LocalDate startDate = startPage == null ? LocalDate.MIN : startPage.getStartDate();
		LocalTime startTime = startPage == null ? LocalTime.MIN : startPage.getStartTime();
		LocalDate endDate = LocalDate.ofInstant(interval.getEnd(), TIMEZONE_OFFSET);
		LocalTime endTime = LocalTime.ofInstant(interval.getEnd(), TIMEZONE_OFFSET);

		return operators.getPageOperator().getPagesForInterval(
				bookId.getName(),
				startDate,
				startTime,
				endDate,
				endTime,
				readAttrs)
				.thenApply(rs -> {
					PagedIterator<PageEntity> pagedIterator = new PagedIterator<>(rs,
							selectExecutor,
							operators.getPageEntityConverter()::getEntity,
							queryInfo);

					return new ConvertingIterator<>(pagedIterator, PageEntity::toPageInfo);
				});
	}

	@Override
	protected Iterator<PageInfo> doGetPages(BookId bookId, Interval interval) throws CradleStorageException {
		String queryInfo = String.format(
				"Getting pages for book %s between  %s - %s ",
				bookId.getName(),
				interval.getStart(),
				interval.getEnd());

		try {
			return doGetPagesAsync(bookId, interval).get();
		} catch (InterruptedException | ExecutionException e) {
			throw new CradleStorageException("Error while " + queryInfo, e);
		}
	}
}

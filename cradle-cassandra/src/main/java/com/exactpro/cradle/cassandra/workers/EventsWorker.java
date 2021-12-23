/*
 * Copyright 2021-2021 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.cradle.cassandra.workers;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.zip.DataFormatException;

import com.exactpro.cradle.BookAndPageChecker;
import com.exactpro.cradle.cassandra.CassandraStorageSettings;
import com.exactpro.cradle.cassandra.dao.testevents.converters.TestEventEntityConverter;
import com.exactpro.cradle.cassandra.retries.SelectQueryExecutor;
import com.exactpro.cradle.utils.CradleIdException;
import com.exactpro.cradle.utils.CradleStorageException;
import io.prometheus.client.Counter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.exactpro.cradle.BookId;
import com.exactpro.cradle.BookInfo;
import com.exactpro.cradle.PageId;
import com.exactpro.cradle.cassandra.dao.BookOperators;
import com.exactpro.cradle.cassandra.dao.CradleOperators;
import com.exactpro.cradle.cassandra.dao.cache.CachedScope;
import com.exactpro.cradle.cassandra.dao.cache.CachedPageScope;
import com.exactpro.cradle.cassandra.dao.testevents.PageScopeEntity;
import com.exactpro.cradle.cassandra.dao.testevents.ScopeEntity;
import com.exactpro.cradle.cassandra.dao.testevents.TestEventEntity;
import com.exactpro.cradle.cassandra.dao.testevents.TestEventIteratorProvider;
import com.exactpro.cradle.cassandra.dao.testevents.TestEventOperator;
import com.exactpro.cradle.cassandra.resultset.CassandraCradleResultSet;
import com.exactpro.cradle.resultset.CradleResultSet;
import com.exactpro.cradle.testevents.StoredTestEvent;
import com.exactpro.cradle.testevents.StoredTestEventId;
import com.exactpro.cradle.testevents.TestEventFilter;
import com.exactpro.cradle.testevents.TestEventToStore;
import com.exactpro.cradle.utils.TimeUtils;

public class EventsWorker extends Worker
{
	private static final Logger logger = LoggerFactory.getLogger(EventsWorker.class);
	
	private static final Counter EVENTS_READ_METRIC = Counter.build().name("cradle_test_events_readed")
			.help("Fetched test events").labelNames(BOOK_ID, SCOPE).register();
	private static final Counter EVENTS_STORE_METRIC = Counter.build().name("cradle_test_events_stored")
			.help("Stored test events").labelNames(BOOK_ID, SCOPE, DIRECTION).register();

	public EventsWorker(CassandraStorageSettings settings, CradleOperators ops,
			ExecutorService composingService, BookAndPageChecker bpc,
			SelectQueryExecutor selectQueryExecutor,
			Function<BoundStatementBuilder, BoundStatementBuilder> writeAttrs,
			Function<BoundStatementBuilder, BoundStatementBuilder> readAttrs)
	{
		super(settings, ops, composingService, bpc, selectQueryExecutor, writeAttrs, readAttrs);
	}

	public static StoredTestEvent mapTestEventEntity(PageId pageId, TestEventEntity entity)
	{
		try
		{
			StoredTestEvent testEvent = entity.toStoredTestEvent(pageId);
			updateEventReadMetrics(testEvent);
			return testEvent;
		}
		catch (DataFormatException | CradleStorageException | CradleIdException | IOException e)
		{
			throw new CompletionException("Error while converting test event entity into Cradle test event", e);
		}
	}
	
	private static void updateEventReadMetrics(StoredTestEvent testEvent)
	{
		EVENTS_READ_METRIC.labels(testEvent.getId().getBookId().getName(), testEvent.getScope())
				.inc(testEvent.isSingle() ? 1 : testEvent.asBatch().getTestEventsCount());
	}

	private static void updateEventWriteMetrics(TestEventEntity entity, BookId bookId)
	{
		EVENTS_STORE_METRIC.labels(bookId.getName(), entity.getScope())
				.inc(entity.getEventCount());
	}

	public TestEventEntity createEntity(TestEventToStore event, PageId pageId) throws IOException
	{
		return new TestEventEntity(event, pageId, settings.getMaxUncompressedTestEventSize());
	}
	
	public CompletableFuture<Void> storeEntity(TestEventEntity entity, BookId bookId)
	{
		TestEventOperator op = getBookOps(bookId).getTestEventOperator();
		return op.write(entity, writeAttrs).thenAcceptAsync(result -> updateEventWriteMetrics(result, bookId));
	}
	
	public CompletableFuture<ScopeEntity> storeScope(TestEventToStore event, BookOperators bookOps)
	{
		if (!bookOps.getScopesCache().store(new CachedScope(bookOps.getBookId().toString(), event.getScope())))
		{
			logger.debug("Skipped writing scope of event '{}'", event.getId());
			return CompletableFuture.completedFuture(null);
		}
		
		logger.debug("Writing scope of event '{}'", event.getId());
		return bookOps.getScopeOperator()
				.write(new ScopeEntity(bookOps.getBookId().getName(), event.getScope()), writeAttrs);
	}
	
	public CompletableFuture<PageScopeEntity> storePageScope(TestEventToStore event, PageId pageId, BookOperators bookOps)
	{
		if (!bookOps.getPageScopesCache().store(new CachedPageScope(pageId.toString(), event.getScope())))
		{
			logger.debug("Skipped writing scope partition of event '{}'", event.getId());
			return CompletableFuture.completedFuture(null);
		}
		
		logger.debug("Writing scope partition of event '{}'", event.getId());
		return bookOps.getPageScopesOperator()
				.write(new PageScopeEntity(pageId.getName(), event.getScope()), writeAttrs);
	}
	
	public CompletableFuture<StoredTestEvent> getTestEvent(StoredTestEventId id, PageId pageId)
	{
		LocalDateTime ldt = TimeUtils.toLocalTimestamp(id.getStartTimestamp());
		BookOperators bookOps = getBookOps(pageId.getBookId());
		TestEventEntityConverter converter = bookOps.getTestEventEntityConverter();
		return selectQueryExecutor.executeSingleRowResultQuery(
				() -> bookOps.getTestEventOperator().get(pageId.getName(), id.getScope(),
						ldt.toLocalDate(), ldt.toLocalTime(), id.getId(), readAttrs), converter::getEntity,
				String.format("getting test event by id '%s'", id))
				.thenApplyAsync(entity -> {
					if (entity == null)
						return null;
					
					try
					{
						return entity.toStoredTestEvent(pageId);
					}
					catch (Exception e)
					{
						throw new CompletionException("Error while converting data of event "+id+" into test event", e);
					}
				}, composingService);
	}
	
	public CompletableFuture<CradleResultSet<StoredTestEvent>> getTestEvents(TestEventFilter filter, BookInfo book)
	{
		TestEventIteratorProvider provider = new TestEventIteratorProvider("get test events filtered by "+filter, 
				filter, getBookOps(filter.getBookId()), book, composingService, selectQueryExecutor, readAttrs);
		return provider.nextIterator()
				.thenApply(r -> new CassandraCradleResultSet<>(r, provider));
	}
	
	public CompletableFuture<Void> updateStatus(StoredTestEvent event, boolean success)
	{
		BookOperators bookOps = getBookOps(event.getBookId());
		StoredTestEventId id = event.getId();
		LocalDateTime ldt = TimeUtils.toLocalTimestamp(event.getStartTimestamp());
		
		return bookOps.getTestEventOperator().updateStatus(event.getPageId().getName(), id.getScope(), 
				ldt.toLocalDate(), ldt.toLocalTime(), id.getId(), success, writeAttrs);
	}
}

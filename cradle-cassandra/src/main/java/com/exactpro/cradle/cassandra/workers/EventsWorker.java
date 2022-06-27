/*
 * Copyright 2021-2022 Exactpro (Exactpro Systems Limited)
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
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.zip.DataFormatException;

import com.exactpro.cradle.*;
import com.exactpro.cradle.cassandra.counters.BookStatisticsRecordsCaches;
import com.exactpro.cradle.cassandra.counters.EntityStatisticsCollector;
import com.exactpro.cradle.cassandra.dao.testevents.converters.TestEventEntityConverter;
import com.exactpro.cradle.serialization.SerializedEntityMetadata;
import com.exactpro.cradle.utils.CradleIdException;
import com.exactpro.cradle.utils.CradleStorageException;
import io.prometheus.client.Counter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.cradle.cassandra.dao.CassandraOperators;
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
			.help("Stored test events").labelNames(BOOK_ID, SCOPE).register();

	private final EntityStatisticsCollector entityStatisticsCollector;
	public EventsWorker(WorkerSupplies workerSupplies, EntityStatisticsCollector entityStatisticsCollector)
	{
		super(workerSupplies);
		this.entityStatisticsCollector = entityStatisticsCollector;
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
				.inc(entity.isEventBatch() ? entity.getEventCount() : 1);
	}

	public TestEventEntity createEntity(TestEventToStore event, PageId pageId) throws IOException
	{
		return new TestEventEntity(event, pageId, settings.getMaxUncompressedTestEventSize());
	}
	
	public CompletableFuture<Void> storeEntity(TestEventEntity entity, BookId bookId)
	{
		TestEventOperator op = getOperators().getTestEventOperator();
		List<SerializedEntityMetadata> meta = entity.getSerializedEventMetadata();
		BookStatisticsRecordsCaches.EntityKey key = new BookStatisticsRecordsCaches.EntityKey(entity.getPage(), EntityType.EVENT);
		return op.write(entity, writeAttrs)
				.thenAccept(result -> entityStatisticsCollector.updateEntityBatchStatistics(bookId, key, meta))
				.thenAcceptAsync(result -> updateEventWriteMetrics(entity, bookId));
	}
	
	public CompletableFuture<ScopeEntity> storeScope(TestEventToStore event)
	{
		String bookName = event.getBookId().getName();
		CassandraOperators operators = getOperators();
		if (!operators.getScopesCache().store(new CachedScope(bookName, event.getScope())))
		{
			logger.debug("Skipped writing scope of event '{}'", event.getId());
			return CompletableFuture.completedFuture(null);
		}
		
		logger.debug("Writing scope of event '{}'", event.getId());
		return operators.getScopeOperator()
				.write(new ScopeEntity(bookName, event.getScope()), writeAttrs);
	}
	
	public CompletableFuture<PageScopeEntity> storePageScope(TestEventToStore event, PageId pageId)
	{
		CassandraOperators operators = getOperators();
		if (!operators.getPageScopesCache().store(
				new CachedPageScope(pageId.getBookId().getName(), pageId.getName(), event.getScope())))
		{
			logger.debug("Skipped writing scope partition of event '{}'", event.getId());
			return CompletableFuture.completedFuture(null);
		}
		
		logger.debug("Writing scope partition of event '{}'", event.getId());
		return operators.getPageScopesOperator()
				.write(new PageScopeEntity(pageId.getBookId().getName(), pageId.getName(), event.getScope()), writeAttrs);
	}
	
	public CompletableFuture<StoredTestEvent> getTestEvent(StoredTestEventId id, PageId pageId)
	{
		LocalDateTime ldt = TimeUtils.toLocalTimestamp(id.getStartTimestamp());
		CassandraOperators operators = getOperators();
		TestEventEntityConverter converter = operators.getTestEventEntityConverter();
		return selectQueryExecutor.executeSingleRowResultQuery(
				() -> operators.getTestEventOperator().get(pageId.getBookId().getName(), pageId.getName(), id.getScope(),
						ldt.toLocalDate(), ldt.toLocalTime(), id.getId(), readAttrs), converter::getEntity,
				String.format("get test event by id '%s'", id))
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
				filter, getOperators(), book, composingService, selectQueryExecutor,
				composeReadAttrs(filter.getFetchParameters()));
		return provider.nextIterator()
				.thenApply(r -> new CassandraCradleResultSet<>(r, provider));
	}
	
	public CompletableFuture<Void> updateStatus(StoredTestEvent event, boolean success)
	{
		StoredTestEventId id = event.getId();
		LocalDateTime ldt = TimeUtils.toLocalTimestamp(event.getStartTimestamp());
		PageId pageId = event.getPageId();
		return getOperators().getTestEventOperator().updateStatus(pageId.getBookId().getName(), pageId.getName(), id.getScope(),
				ldt.toLocalDate(), ldt.toLocalTime(), id.getId(), success, writeAttrs);
	}
}

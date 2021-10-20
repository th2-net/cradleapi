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

package com.exactpro.cradle.cassandra;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Function;

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
import com.exactpro.cradle.cassandra.dao.testevents.EventEntityUtils;
import com.exactpro.cradle.cassandra.dao.testevents.PageScopeEntity;
import com.exactpro.cradle.cassandra.dao.testevents.ScopeEntity;
import com.exactpro.cradle.cassandra.dao.testevents.TestEventEntity;
import com.exactpro.cradle.cassandra.dao.testevents.TestEventIteratorProvider;
import com.exactpro.cradle.cassandra.dao.testevents.TestEventOperator;
import com.exactpro.cradle.cassandra.resultset.CassandraCradleResultSet;
import com.exactpro.cradle.cassandra.utils.CassandraTimeUtils;
import com.exactpro.cradle.resultset.CradleResultSet;
import com.exactpro.cradle.testevents.StoredTestEvent;
import com.exactpro.cradle.testevents.StoredTestEventId;
import com.exactpro.cradle.testevents.TestEventFilter;
import com.exactpro.cradle.testevents.TestEventToStore;
import com.exactpro.cradle.utils.CradleStorageException;
import com.exactpro.cradle.utils.TimeUtils;

public class EventsWorker
{
	private static final Logger logger = LoggerFactory.getLogger(EventsWorker.class);
	
	private final CassandraStorageSettings settings;
	private final CradleOperators ops;
	private final Function<BoundStatementBuilder, BoundStatementBuilder> writeAttrs,
			readAttrs;
	
	public EventsWorker(CassandraStorageSettings settings, CradleOperators ops, 
			Function<BoundStatementBuilder, BoundStatementBuilder> writeAttrs,
			Function<BoundStatementBuilder, BoundStatementBuilder> readAttrs)
	{
		this.settings = settings;
		this.ops = ops;
		this.writeAttrs = writeAttrs;
		this.readAttrs = readAttrs;
	}
	
	
	public List<TestEventEntity> createEntities(TestEventToStore event, PageId pageId) throws IOException
	{
		return EventEntityUtils.toEntities(event, pageId, 
				settings.getMaxUncompressedTestEventSize(), settings.getTestEventChunkSize(), settings.getTestEventMessagesPerChunk());
	}
	
	public CompletableFuture<TestEventEntity> storeEntities(Collection<TestEventEntity> entities, BookId bookId)
	{
		TestEventOperator op = getBookOps(bookId).getTestEventOperator();
		
		CompletableFuture<TestEventEntity> result = null;
		for (TestEventEntity ent : entities)
		{
			if (result == null)
				result = op.write(ent, writeAttrs);
			else
				result = result.thenComposeAsync(r -> op.write(ent, writeAttrs));
		}
		return result;
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
		LocalDateTime ldt = TimeUtils.toLocalTimestamp(event.getStartTimestamp());
		if (!bookOps.getPageScopesCache().store(new CachedPageScope(pageId.toString(), event.getScope(), CassandraTimeUtils.getPart(ldt))))
		{
			logger.debug("Skipped writing scope partition of event '{}'", event.getId());
			return CompletableFuture.completedFuture(null);
		}
		
		logger.debug("Writing scope partition of event '{}'", event.getId());
		return bookOps.getPageScopesOperator()
				.write(new PageScopeEntity(pageId.getName(), event.getScope(), CassandraTimeUtils.getPart(ldt)), writeAttrs);
	}
	
	public CompletableFuture<StoredTestEvent> getTestEvent(StoredTestEventId id, PageId pageId)
	{
		LocalDateTime ldt = TimeUtils.toLocalTimestamp(id.getStartTimestamp());
		LocalTime lt = ldt.toLocalTime();
		BookId bookId = pageId.getBookId();
		return getBookOps(bookId).getTestEventOperator().get(pageId.getName(), ldt.toLocalDate(), id.getScope(), CassandraTimeUtils.getPart(ldt), 
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
	
	public CompletableFuture<CradleResultSet<StoredTestEvent>> getTestEvents(TestEventFilter filter, BookInfo book)
	{
		TestEventIteratorProvider provider = new TestEventIteratorProvider("get test events filtered by "+filter, 
				filter, getBookOps(filter.getBookId()), book, readAttrs);
		return provider.nextIterator()
				.thenApplyAsync(r -> new CassandraCradleResultSet<>(r, provider));
	}
	
	public CompletableFuture<Void> updateStatus(StoredTestEvent event, boolean success)
	{
		BookOperators bookOps = getBookOps(event.getBookId());
		StoredTestEventId id = event.getId();
		LocalDateTime ldt = TimeUtils.toLocalTimestamp(event.getStartTimestamp());
		LocalDate ld = ldt.toLocalDate();
		LocalTime lt = ldt.toLocalTime();
		
		return bookOps.getTestEventOperator().updateStatus(event.getPageId().getName(), id.getScope(), CassandraTimeUtils.getPart(ldt),
				ld, lt, id.getId(), success, writeAttrs);
	}
	
	
	private BookOperators getBookOps(BookId bookId)
	{
		try
		{
			return ops.getOperators(bookId);
		}
		catch (CradleStorageException e)
		{
			throw new CompletionException(e);
		}
	}
}

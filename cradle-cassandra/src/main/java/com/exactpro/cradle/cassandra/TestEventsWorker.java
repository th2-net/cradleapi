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

import static java.util.stream.Collectors.toList;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.exactpro.cradle.cassandra.dao.CassandraOperators;
import com.exactpro.cradle.cassandra.dao.testevents.DetailedTestEventEntity;
import com.exactpro.cradle.cassandra.dao.testevents.RootTestEventEntity;
import com.exactpro.cradle.cassandra.dao.testevents.TestEventChildDateEntity;
import com.exactpro.cradle.cassandra.dao.testevents.TestEventChildEntity;
import com.exactpro.cradle.cassandra.dao.testevents.TimeTestEventEntity;
import com.exactpro.cradle.cassandra.iterators.RootTestEventsMetadataIteratorAdapter;
import com.exactpro.cradle.cassandra.iterators.TestEventChildrenMetadataIteratorAdapter;
import com.exactpro.cradle.cassandra.iterators.TestEventDataIteratorAdapter;
import com.exactpro.cradle.cassandra.iterators.TimeTestEventsMetadataIteratorAdapter;
import com.exactpro.cradle.cassandra.utils.DateTimeUtils;
import com.exactpro.cradle.cassandra.utils.TimestampRange;
import com.exactpro.cradle.testevents.StoredTestEvent;
import com.exactpro.cradle.testevents.StoredTestEventId;
import com.exactpro.cradle.testevents.StoredTestEventMetadata;
import com.exactpro.cradle.testevents.StoredTestEventWrapper;

public class TestEventsWorker
{
	private static final Logger logger = LoggerFactory.getLogger(TestEventsWorker.class);
	
	private final UUID instanceUuid;
	private final CassandraOperators ops;
	private final Function<BoundStatementBuilder, BoundStatementBuilder> writeAttrs,
			readAttrs;
	private final ExecutorService composingService;
	
	public TestEventsWorker(UUID instanceUuid, CassandraOperators ops, 
			Function<BoundStatementBuilder, BoundStatementBuilder> writeAttrs,
			Function<BoundStatementBuilder, BoundStatementBuilder> readAttrs,
			ExecutorService composingService)
	{
		this.instanceUuid = instanceUuid;
		this.ops = ops;
		this.writeAttrs = writeAttrs;
		this.readAttrs = readAttrs;
		this.composingService = composingService;
	}
	
	
	public CompletableFuture<?> writeEvent(DetailedTestEventEntity entity, StoredTestEvent event)
	{
		return ops.getTestEventOperator().write(entity, writeAttrs)
				.thenComposeAsync(r -> writeTimeEvent(event), composingService)
				.thenComposeAsync(r -> {
					if (event.getParentId() != null)
					{
						return writeEventInParent(event)
								.thenComposeAsync(r2 -> writeEventDateInParent(event), composingService)
								.thenComposeAsync(r2 -> updateParentTestEvents(event), composingService)
								.thenAccept(r2 -> {});
					}
					else
						return writeRootEvent(event).thenAccept(r2 -> {});
				}, composingService);
	}
	
	public CompletableFuture<Void> updateStatus(StoredTestEventWrapper event, boolean success)
	{
		String id = event.getId().toString(),
				parentId = event.getParentId() != null ? event.getParentId().toString() : null;
		LocalDateTime ldt = DateTimeUtils.toDateTime(event.getStartTimestamp());
		LocalDate ld = ldt.toLocalDate();
		LocalTime lt = ldt.toLocalTime();
		
		return ops.getTestEventOperator().updateStatus(instanceUuid, id, success, writeAttrs)
				.thenComposeAsync(r -> ops.getTimeTestEventOperator().updateStatus(instanceUuid, ld, lt, id, success, writeAttrs), composingService)
				.thenComposeAsync(r -> {
					if (parentId != null)
						return ops.getTestEventChildrenOperator().updateStatus(instanceUuid, parentId, ld, lt, id, success, writeAttrs);
					return ops.getRootTestEventOperator().updateStatus(instanceUuid, ld, lt, id, success, writeAttrs);
				}, composingService)
				.thenAccept(r -> {});
	}
	
	
	public CompletableFuture<StoredTestEventWrapper> readEvent(StoredTestEventId id)
	{
		return ops.getTestEventOperator().get(instanceUuid, id.toString(), readAttrs)
				.thenApplyAsync(entity -> {
					try
					{
						return entity == null ? null : entity.toStoredTestEventWrapper();
					}
					catch (Exception error)
					{
						throw new CompletionException("Could not get test event", error);
					}
				}, composingService);
	}
	
	public CompletableFuture<Iterable<StoredTestEventWrapper>> readCompleteEvents(Set<StoredTestEventId> ids)
	{
		return ops.getTestEventOperator().getComplete(instanceUuid, 
				ids.stream().map(StoredTestEventId::toString).collect(toList()), readAttrs)
				.thenApply(TestEventDataIteratorAdapter::new);
	}
	
	public CompletableFuture<Iterable<StoredTestEventMetadata>> readRootEvents(TimestampRange range)
	{
		LocalTime fromTime = range.getFrom().toLocalTime(),
				toTime = range.getTo().toLocalTime();
		return ops.getRootTestEventOperator().getTestEvents(instanceUuid, range.getFrom().toLocalDate(), fromTime, toTime, readAttrs)
				.thenApply(RootTestEventsMetadataIteratorAdapter::new);
	}
	
	public CompletableFuture<Iterable<StoredTestEventMetadata>> readEvents(StoredTestEventId parentId, TimestampRange range)
	{
		LocalTime fromTime = range.getFrom().toLocalTime(),
				toTime = range.getTo().toLocalTime();
		return ops.getTestEventChildrenOperator().getTestEvents(instanceUuid, parentId.toString(), range.getFrom().toLocalDate(), fromTime, toTime, readAttrs)
				.thenApply(TestEventChildrenMetadataIteratorAdapter::new);
	}
	
	public CompletableFuture<Iterable<StoredTestEventMetadata>> readEvents(TimestampRange range)
	{
		LocalTime fromTime = range.getFrom().toLocalTime(),
				toTime = range.getTo().toLocalTime();
		return ops.getTimeTestEventOperator().getTestEvents(instanceUuid, range.getFrom().toLocalDate(), fromTime, toTime, readAttrs)
				.thenApply(TimeTestEventsMetadataIteratorAdapter::new);
	}
	
	
	private CompletableFuture<TimeTestEventEntity> writeTimeEvent(StoredTestEvent event)
	{
		TimeTestEventEntity timeEntity;
		try
		{
			timeEntity = new TimeTestEventEntity(event, instanceUuid);
		}
		catch (IOException e)
		{
			CompletableFuture<TimeTestEventEntity> error = new CompletableFuture<>();
			error.completeExceptionally(e);
			return error;
		}
		
		logger.trace("Executing time/event storing query for {}", event.getId());
		return ops.getTimeTestEventOperator().writeTestEvent(timeEntity, writeAttrs);
	}
	
	private CompletableFuture<RootTestEventEntity> writeRootEvent(StoredTestEvent event)
	{
		RootTestEventEntity entity = new RootTestEventEntity(event, instanceUuid);
		
		logger.trace("Executing root event storing query for {}", event.getId());
		return ops.getRootTestEventOperator().writeTestEvent(entity, writeAttrs);
	}
	
	private CompletableFuture<TestEventChildEntity> writeEventInParent(StoredTestEvent event)
	{
		TestEventChildEntity entity;
		try
		{
			entity = new TestEventChildEntity(event, instanceUuid);
		}
		catch (IOException e)
		{
			CompletableFuture<TestEventChildEntity> error = new CompletableFuture<>();
			error.completeExceptionally(e);
			return error;
		}
		
		logger.trace("Executing parent/event storing query for {}", event.getId());
		return ops.getTestEventChildrenOperator().writeTestEvent(entity, writeAttrs);
	}
	
	private CompletableFuture<TestEventChildDateEntity> writeEventDateInParent(StoredTestEvent event)
	{
		TestEventChildDateEntity entity = new TestEventChildDateEntity(event, instanceUuid);
		
		logger.trace("Executing parent/event date storing query for {}", event.getId());
		return ops.getTestEventChildrenDatesOperator().writeTestEventDate(entity, writeAttrs);
	}
	
	private CompletableFuture<Void> updateParentTestEvents(StoredTestEvent event)
	{
		if (event.isSuccess())
			return CompletableFuture.completedFuture(null);
		
		logger.trace("Updating parent of {}", event.getId());
		return failEventAndParents(event.getParentId());
	}
	
	private CompletableFuture<Void> failEventAndParents(StoredTestEventId eventId)
	{
		return readEvent(eventId)
				.thenComposeAsync((event) -> {
					if (event == null || !event.isSuccess())  //Invalid event ID or event is already failed, which means that its parents are already updated
						return CompletableFuture.completedFuture(null);
					
					CompletableFuture<Void> update = updateStatus(event, false);
					if (event.getParentId() != null)
						return update.thenComposeAsync((u) -> failEventAndParents(event.getParentId()), composingService);
					return update;
				}, composingService);
	}
}
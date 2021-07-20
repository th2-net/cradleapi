/*
 * Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.cradle.cassandra.dao.messages;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.MappedAsyncPagingIterable;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.mapper.MapperContext;
import com.datastax.oss.driver.api.mapper.entity.EntityHelper;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.exactpro.cradle.cassandra.utils.CassandraMessageUtils;
import com.exactpro.cradle.cassandra.utils.FilterUtils;
import com.exactpro.cradle.filters.ComparisonOperation;
import com.exactpro.cradle.messages.StoredMessageFilter;
import com.exactpro.cradle.messages.StoredMessageId;
import com.exactpro.cradle.utils.CradleStorageException;

import static com.exactpro.cradle.cassandra.StorageConstants.*;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.*;

public class MessageBatchQueryProvider
{
	private static final Logger logger = LoggerFactory.getLogger(MessageBatchQueryProvider.class);
	
	private static final String LEFT_MESSAGE_INDEX = "left_"+MESSAGE_INDEX,
			RIGHT_MESSAGE_INDEX = "right_"+MESSAGE_INDEX;
	private final CqlSession session;
	private final EntityHelper<DetailedMessageBatchEntity> helper;
	private final Select selectStart;
	
	public MessageBatchQueryProvider(MapperContext context, EntityHelper<DetailedMessageBatchEntity> helper)
	{
		this.session = context.getSession();
		this.helper = helper;
		this.selectStart = helper.selectStart()
				.whereColumn(INSTANCE_ID).isEqualTo(bindMarker())
				.allowFiltering();
	}
	
	public CompletableFuture<MappedAsyncPagingIterable<DetailedMessageBatchEntity>> filterMessages(UUID instanceId, StoredMessageFilter filter, 
			MessageBatchOperator operator, Function<BoundStatementBuilder, BoundStatementBuilder> attributes)
	{
		Select select = selectStart;
		if (filter != null)
			select = addFilter(select, filter);
		
		PreparedStatement ps = session.prepare(select.build());
		BoundStatement bs;
		try
		{
			bs = bindParameters(ps, instanceId, filter, operator, attributes);
		}
		catch (CradleStorageException e)
		{
			CompletableFuture<MappedAsyncPagingIterable<DetailedMessageBatchEntity>> error = new CompletableFuture<>();
			error.completeExceptionally(e);
			return error;
		}
		return session.executeAsync(bs).thenApply((r) -> r.map(helper::get)).toCompletableFuture();
	}
	
	private Select addFilter(Select select, StoredMessageFilter filter)
	{
		if (filter.getStreamName() != null)
			select = FilterUtils.filterToWhere(filter.getStreamName().getOperation(), select.whereColumn(STREAM_NAME), null);
		
		if (filter.getDirection() != null)
			select = FilterUtils.filterToWhere(filter.getDirection().getOperation(), select.whereColumn(DIRECTION), null);
		
		if (filter.getIndex() != null)
		{
			ComparisonOperation operation = filter.getIndex().getOperation();
			//This is for case when need to return "previous X messages, i.e. X messages whose index is less than Y"
			if (filter.getLimit() > 0 && (operation == ComparisonOperation.LESS || operation == ComparisonOperation.LESS_OR_EQUALS))
				select = FilterUtils.filterToWhere(ComparisonOperation.GREATER_OR_EQUALS, select.whereColumn(MESSAGE_INDEX), LEFT_MESSAGE_INDEX);
			
			//Overriding operation to include message's batch while selecting by query
			//While iterating through query results original operation will be used
			if (operation == ComparisonOperation.GREATER)
				operation = ComparisonOperation.GREATER_OR_EQUALS;
			else if (operation == ComparisonOperation.LESS)
				operation = ComparisonOperation.LESS_OR_EQUALS;
			select = FilterUtils.filterToWhere(operation, select.whereColumn(MESSAGE_INDEX), RIGHT_MESSAGE_INDEX);
		}
		
		//For both timestamp comparisons overriding operation with GreaterOrEquals/LessOrEquals for date portion to not skip date of timestamp 
		//when Greater/Less (not GreaterOrEquals/LessOrEquals) is defined for condition timestamp
		//E.g. in batch first_message_date=2020-05-26, first_message_time=20:23:00
		//condition timestampFrom>2020-05-26 15:00:00
		//condition to use will be first_message_date>2020-05-26, first_message_time>15:00:00
		//Without override of operation for date portion this batch will be skipped
		//So, override makes the condition the following:
		//first_message_date>=2020-05-26, first_message_time>15:00:00
		if (filter.getTimestampFrom() != null)
		{
			ComparisonOperation op = filter.getTimestampFrom().getOperation();
			select = FilterUtils.filterToWhere(ComparisonOperation.GREATER_OR_EQUALS, select.whereColumn(FIRST_MESSAGE_DATE), null);
			select = FilterUtils.filterToWhere(op, select.whereColumn(FIRST_MESSAGE_TIME), null);
		}
		
		if (filter.getTimestampTo() != null)
		{
			ComparisonOperation op = filter.getTimestampTo().getOperation();
			select = FilterUtils.filterToWhere(ComparisonOperation.LESS_OR_EQUALS, select.whereColumn(LAST_MESSAGE_DATE), null);
			select = FilterUtils.filterToWhere(op, select.whereColumn(LAST_MESSAGE_TIME), null);
		}
		
		if (filter.getLimit() > 0)
			select.limit(filter.getLimit());
		
		return select;
	}
	
	private BoundStatement bindParameters(PreparedStatement ps, UUID instanceId, StoredMessageFilter filter, 
			MessageBatchOperator operator, Function<BoundStatementBuilder, BoundStatementBuilder> attributes) throws CradleStorageException
	{
		BoundStatementBuilder builder = ps.boundStatementBuilder()
				.setUuid(INSTANCE_ID, instanceId);
		builder = attributes.apply(builder);
		if (filter != null)
			builder = bindFilterParameters(builder, instanceId, filter, operator, attributes);
		return builder.build();
	}
	
	private DetailedMessageBatchEntity getMessageBatch(UUID instanceId, StoredMessageFilter filter, 
			MessageBatchOperator operator, Function<BoundStatementBuilder, BoundStatementBuilder> attributes) throws CradleStorageException
	{
		if (filter.getStreamName() == null || filter.getDirection() == null)
		{
			//FIXME: throw exception to require stream name and direction to filter by message index
			//throw new CradleStorageException("Both streamName and direction are required when filtering by message index");
			return null;
		}
		
		StoredMessageId id = new StoredMessageId(filter.getStreamName().getValue(), 
				filter.getDirection().getValue(), 
				filter.getIndex().getValue());
		try
		{
			return CassandraMessageUtils.getMessageBatch(id, operator, instanceId, attributes).get();
		} catch (InterruptedException | ExecutionException e)
		{
			throw new CradleStorageException("Error while getting message batch for ID "+id, e);
		}
	}
	
	private BoundStatementBuilder bindFilterParameters(BoundStatementBuilder builder, UUID instanceId, StoredMessageFilter filter, 
			MessageBatchOperator operator, Function<BoundStatementBuilder, BoundStatementBuilder> attributes) throws CradleStorageException
	{
		if (filter.getStreamName() != null)
			builder = builder.setString(STREAM_NAME, filter.getStreamName().getValue());
		
		if (filter.getDirection() != null)
			builder = builder.setString(DIRECTION, filter.getDirection().getValue().getLabel());
		
		if (filter.getIndex() != null)
		{
			DetailedMessageBatchEntity batch = getMessageBatch(instanceId, filter, operator, attributes);
			
			ComparisonOperation op = filter.getIndex().getOperation();
			if (filter.getLimit() > 0 && (op == ComparisonOperation.LESS || op == ComparisonOperation.LESS_OR_EQUALS))
			{
				long leftBatchIndex;
				try
				{
  				//Finding left bound for filter (will use it in iterator) and batch index (will use it in query)
					leftBatchIndex = CassandraMessageUtils.findLeftMessageIndex(batch, filter, instanceId, operator, attributes);
				}
				catch (IOException e)
				{
					logger.warn("Error while finding left batch index for stream "
							+ "'"+batch.getStreamName()+"', direction '"+batch.getDirection()+"' and index "+batch.getMessageIndex(), e);
					leftBatchIndex = batch.getMessageIndex();
				}
				builder = builder.setLong(LEFT_MESSAGE_INDEX, leftBatchIndex); 
			}
			builder = builder.setLong(RIGHT_MESSAGE_INDEX, batch != null ? batch.getMessageIndex() : filter.getIndex().getValue());
		}
		
		//Both filters for timestamp are adjusted to get more batches than we will get in case of strict comparison.
		//This is to cover the case when messages that meet condition timestamp are in the middle of the batch and the batch is long in terms of time.
		//E.g. in batch first_message_time=15:00:00, last_message_time=15:05:00, 
		//condition timestampFrom>15:01:00
		//Without condition adjustment we'll skip such batch thus not showing messages that actually met the condition
		//FIXME: This doesn't guarantee that batches longer than 10 minutes will be not skipped! Anyway, such batches are bad.
		if (filter.getTimestampFrom() != null)
		{
			Instant ts = filter.getTimestampFrom().getValue();
			builder = FilterUtils.bindTimestamp(ts.minus(10, ChronoUnit.MINUTES), builder, FIRST_MESSAGE_DATE, FIRST_MESSAGE_TIME);
		}
		
		if (filter.getTimestampTo() != null)
		{
			Instant ts = filter.getTimestampTo().getValue();
			builder = FilterUtils.bindTimestamp(ts.plus(10, ChronoUnit.MINUTES), builder, LAST_MESSAGE_DATE, LAST_MESSAGE_TIME);
		}
		
		return builder;
	}
}

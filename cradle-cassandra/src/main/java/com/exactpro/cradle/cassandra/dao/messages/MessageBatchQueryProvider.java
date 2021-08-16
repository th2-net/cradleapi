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

package com.exactpro.cradle.cassandra.dao.messages;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.MappedAsyncPagingIterable;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.mapper.MapperContext;
import com.datastax.oss.driver.api.mapper.entity.EntityHelper;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.exactpro.cradle.Direction;
import com.exactpro.cradle.Order;
import com.exactpro.cradle.cassandra.utils.CassandraMessageUtils;
import com.exactpro.cradle.cassandra.utils.FilterUtils;
import com.exactpro.cradle.filters.ComparisonOperation;
import com.exactpro.cradle.filters.FilterForEquals;
import com.exactpro.cradle.messages.StoredMessageFilter;
import com.exactpro.cradle.messages.StoredMessageId;
import com.exactpro.cradle.utils.CradleStorageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;
import static com.exactpro.cradle.cassandra.CassandraCradleStorage.TIMEZONE_OFFSET;
import static com.exactpro.cradle.cassandra.StorageConstants.*;

public class MessageBatchQueryProvider
{
	private static final Logger logger = LoggerFactory.getLogger(MessageBatchQueryProvider.class);

	private static final String LEFT_MESSAGE_INDEX = "left_" + MESSAGE_INDEX,
			RIGHT_MESSAGE_INDEX = "right_" + MESSAGE_INDEX;
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
	
	public CompletableFuture<MappedAsyncPagingIterable<DetailedMessageBatchEntity>> filterMessages(UUID instanceId, 
			StoredMessageFilter filter, MessageBatchOperator mbOperator,
			TimeMessageOperator tmOperator, Function<BoundStatementBuilder, BoundStatementBuilder> attributes)
	{
		Select select = selectStart;
		Order order = null;
		if (filter != null)
		{
			order = filter.getOrder();
			select = addFilter(select, filter);
		}
		select = orderBy(order, select);
		PreparedStatement ps = session.prepare(select.build());
		BoundStatement bs;
		try
		{
			bs = bindParameters(ps, instanceId, filter, mbOperator, tmOperator, attributes);
		}
		catch (CradleStorageException e)
		{
			CompletableFuture<MappedAsyncPagingIterable<DetailedMessageBatchEntity>> error = new CompletableFuture<>();
			error.completeExceptionally(e);
			return error;
		}
		return session.executeAsync(bs).thenApply(r -> r.map(helper::get)).toCompletableFuture();
	}

	private Select orderBy(Order order, Select select)
	{
		if (order == null)
			order = Order.DIRECT;
		ClusteringOrder clusteringOrder = order == Order.DIRECT ? ClusteringOrder.ASC : ClusteringOrder.DESC;
		return select.orderBy(DIRECTION, clusteringOrder).orderBy(MESSAGE_INDEX, clusteringOrder);
	}

	private Select addFilter(Select select, StoredMessageFilter filter)
	{
		FilterForEquals<String> streamName = filter.getStreamName();
		if (streamName != null)
			select = FilterUtils.filterToWhere(streamName.getOperation(), select.whereColumn(STREAM_NAME), null);

		FilterForEquals<Direction> direction = filter.getDirection();
		if (direction != null)
			select = FilterUtils.filterToWhere(direction.getOperation(), select.whereColumn(DIRECTION), null);

		boolean isLeftIndexSelected = false;
		boolean isRightIndexSelected = false;
		if (filter.getIndex() != null)
		{
			ComparisonOperation op = filter.getIndex().getOperation();
			if (op == ComparisonOperation.EQUALS)
			{
				select = FilterUtils.filterToWhere(ComparisonOperation.GREATER_OR_EQUALS,
						select.whereColumn(MESSAGE_INDEX), LEFT_MESSAGE_INDEX);
				select = FilterUtils.filterToWhere(ComparisonOperation.LESS_OR_EQUALS,
						select.whereColumn(MESSAGE_INDEX), RIGHT_MESSAGE_INDEX);
				return select;
			}
			
			//This is for case when need to return "previous X messages, i.e. X messages whose index is less than Y"
			if (filter.getLimit() > 0 && (op == ComparisonOperation.LESS || op == ComparisonOperation.LESS_OR_EQUALS))
			{
				select = FilterUtils.filterToWhere(ComparisonOperation.GREATER_OR_EQUALS,
						select.whereColumn(MESSAGE_INDEX), LEFT_MESSAGE_INDEX);
			}
			
			//Overriding operation to include message's batch while selecting by query
			//While iterating through query results original op will be used
			switch (op)
			{
				case GREATER:
					op = ComparisonOperation.GREATER_OR_EQUALS;
				case GREATER_OR_EQUALS:
					isLeftIndexSelected = true;
					select = FilterUtils.filterToWhere(op, select.whereColumn(MESSAGE_INDEX), LEFT_MESSAGE_INDEX);
					break;
				case LESS:
					op = ComparisonOperation.LESS_OR_EQUALS;
				case LESS_OR_EQUALS:
				default:
					isRightIndexSelected = true;
					select = FilterUtils.filterToWhere(op, select.whereColumn(MESSAGE_INDEX), RIGHT_MESSAGE_INDEX);
			}
		}
		
		if (!isLeftIndexSelected && filter.getTimestampFrom() != null)
			select = FilterUtils.filterToWhere(ComparisonOperation.GREATER_OR_EQUALS, select.whereColumn(MESSAGE_INDEX),
					LEFT_MESSAGE_INDEX);
		
		if (!isRightIndexSelected && filter.getTimestampTo() != null)
			select = FilterUtils.filterToWhere(ComparisonOperation.LESS_OR_EQUALS, select.whereColumn(MESSAGE_INDEX),
					RIGHT_MESSAGE_INDEX);
		
		if (filter.getLimit() > 0)
			select.limit(filter.getLimit());
		
		return select;
	}

	private BoundStatement bindParameters(PreparedStatement ps, UUID instanceId, StoredMessageFilter filter,
			MessageBatchOperator mbOperator, TimeMessageOperator tmOperator,
			Function<BoundStatementBuilder, BoundStatementBuilder> attributes) throws CradleStorageException
	{
		BoundStatementBuilder builder = ps.boundStatementBuilder().setUuid(INSTANCE_ID, instanceId);
		builder = attributes.apply(builder);
		if (filter != null)
			builder = bindFilterParameters(builder, instanceId, filter, mbOperator, tmOperator, attributes);
		return builder.build();
	}
	
	private DetailedMessageBatchEntity getMessageBatch(UUID instanceId, StoredMessageFilter filter, 
			MessageBatchOperator operator, 
			Function<BoundStatementBuilder, BoundStatementBuilder> attributes) throws CradleStorageException
	{
		if (filter.getStreamName() == null || filter.getDirection() == null)
		{
			//FIXME: throw exception to require stream name and direction to filter by message index
			throw new CradleStorageException("Both streamName and direction are required when filtering by message index");
		}
		
		StoredMessageId id = new StoredMessageId(filter.getStreamName().getValue(), 
				filter.getDirection().getValue(), 
				filter.getIndex().getValue());
		try
		{
			return CassandraMessageUtils.getMessageBatch(id,
					operator, instanceId, attributes).get();
		} catch (InterruptedException | ExecutionException e)
		{
			throw new CradleStorageException("Error while getting message batch for ID "+id, e);
		}
	}

	private BoundStatementBuilder bindFilterParameters(BoundStatementBuilder builder, UUID instanceId,
			StoredMessageFilter filter, MessageBatchOperator operator,
			TimeMessageOperator tmOperator, Function<BoundStatementBuilder, BoundStatementBuilder> attributes)
			throws CradleStorageException
	{
		if (filter.getStreamName() == null)
			throw new CradleStorageException("Stream name is a mandatory filter field and can't be empty");
		
		builder = builder.setString(STREAM_NAME, filter.getStreamName().getValue());

		FilterForEquals<Direction> directionFilter = filter.getDirection();
		if ((filter.getTimestampFrom() != null || filter.getTimestampTo() != null) && directionFilter == null)
			throw new CradleStorageException("Direction is a mandatory filter field for filtering by timestamp or index");

		if (directionFilter != null)
			builder = builder.setString(DIRECTION, directionFilter.getValue().getLabel());

		long leftIndex = 0L;
		long rightIndex = Long.MAX_VALUE;
		if (filter.getIndex() != null)
		{
			ComparisonOperation op = filter.getIndex().getOperation();
			DetailedMessageBatchEntity batch = getMessageBatch(instanceId, filter, operator, attributes);
			
			if (filter.getLimit() > 0 && (op == ComparisonOperation.LESS || op == ComparisonOperation.LESS_OR_EQUALS))
			{
				try
				{
					//Finding left bound for filter (will use it in iterator) and batch index (will use it in query)
					leftIndex = CassandraMessageUtils.findLeftMessageIndex(batch, filter, instanceId, operator, attributes);
				}
				catch (IOException e)
				{
					logger.warn("Error while finding left batch index for stream "
							+ "'"+batch.getStreamName()+"', direction '"+batch.getDirection()+"' and index "+batch.getMessageIndex(), e);
					leftIndex = batch.getMessageIndex();
				}
				builder = builder.setLong(LEFT_MESSAGE_INDEX, leftIndex); 
			}
			long leftBatchIndex = batch != null ? batch.getMessageIndex() : filter.getIndex().getValue();
			long rightBatchIndex = batch != null ? batch.getLastMessageIndex() : filter.getIndex().getValue();
			switch (op)
			{
				case GREATER:
				case GREATER_OR_EQUALS:
					builder = builder.setLong(LEFT_MESSAGE_INDEX, leftIndex = leftBatchIndex);
					break;
				case LESS:
				case LESS_OR_EQUALS:
					builder = builder.setLong(RIGHT_MESSAGE_INDEX, rightIndex = rightBatchIndex);
					break;
				case EQUALS:
					builder = builder.setLong(LEFT_MESSAGE_INDEX, leftBatchIndex);
					builder = builder.setLong(RIGHT_MESSAGE_INDEX, rightBatchIndex); 
				return builder;	
			}
		}
		
		if (filter.getTimestampFrom() != null)
		{
			Instant ts = filter.getTimestampFrom().getValue();
			try
			{
				long fromIndex = getNearestMessageIndexBefore(tmOperator, instanceId,
						filter.getStreamName().getValue(), directionFilter.getValue(), ts, attributes);
				if (fromIndex > leftIndex)
					builder = builder.setLong(LEFT_MESSAGE_INDEX, fromIndex);
			}
			catch (ExecutionException | InterruptedException e)
			{
				throw new CradleStorageException("Error getting message batch index for timestamp 'From=" + ts + '\'', e);
			}
		}

		if (filter.getTimestampTo() != null)
		{
			Instant ts = filter.getTimestampTo().getValue();
			try
			{
				long toIndex = getNearestMessageIndexAfter(tmOperator, instanceId,
						filter.getStreamName().getValue(), directionFilter.getValue(), ts, attributes);
				if (toIndex < rightIndex)
					builder = builder.setLong(RIGHT_MESSAGE_INDEX, toIndex);
			}
			catch (ExecutionException | InterruptedException e)
			{
				throw new CradleStorageException("Error getting message batch index for timestamp 'To=" + ts + '\'', e);
			}
		}
		
		return builder;
	}

	private long getNearestMessageIndexBefore(TimeMessageOperator tmOperator, UUID instanceId, String streamName,
			Direction direction, Instant instant, Function<BoundStatementBuilder, BoundStatementBuilder> attributes)
			throws ExecutionException, InterruptedException
	{
		LocalDateTime ldt = LocalDateTime.ofInstant(instant, TIMEZONE_OFFSET);
		TimeMessageEntity entity = tmOperator.getNearestMessageBefore(instanceId, streamName, ldt.toLocalDate(),
				direction.getLabel(), ldt.toLocalTime(), attributes).get();
		return entity == null ? 0 : entity.getMessageIndex();
	}
	
	private long getNearestMessageIndexAfter(TimeMessageOperator tmOperator, UUID instanceId, String streamName,
			Direction direction, Instant instant, Function<BoundStatementBuilder, BoundStatementBuilder> attributes)
			throws ExecutionException, InterruptedException
	{
		LocalDateTime ldt = LocalDateTime.ofInstant(instant, TIMEZONE_OFFSET);
		TimeMessageEntity entity = tmOperator.getNearestMessageAfter(instanceId, streamName, ldt.toLocalDate(),
				direction.getLabel(), ldt.toLocalTime(), attributes).get();
		return entity == null ? Long.MAX_VALUE : entity.getMessageIndex();
	}
}

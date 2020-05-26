/******************************************************************************
 * Copyright (c) 2009-2020, Exactpro Systems LLC
 * www.exactpro.com
 * Build Software to Test Software
 *
 * All rights reserved.
 * This is unpublished, licensed software, confidential and proprietary 
 * information which is the property of Exactpro Systems LLC or its licensors.
 ******************************************************************************/

package com.exactpro.cradle.cassandra.dao.messages;

import java.util.UUID;
import java.util.function.Function;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.PagingIterable;
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

import static com.exactpro.cradle.cassandra.StorageConstants.*;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.*;

public class MessageBatchQueryProvider
{
	private final CqlSession session;
	private final EntityHelper<MessageBatchEntity> helper;
	private final Select selectStart;
	
	public MessageBatchQueryProvider(MapperContext context, EntityHelper<MessageBatchEntity> helper)
	{
		this.session = context.getSession();
		this.helper = helper;
		this.selectStart = helper.selectStart()
				.whereColumn(INSTANCE_ID).isEqualTo(bindMarker())
				.allowFiltering();
	}
	
	public PagingIterable<MessageBatchEntity> filterMessages(UUID instanceId, StoredMessageFilter filter, MessageBatchOperator operator,
			Function<BoundStatementBuilder, BoundStatementBuilder> attributes)
	{
		Select select = selectStart;
		if (filter != null)
			select = addFilter(select, filter);
		
		PreparedStatement ps = session.prepare(select.build());
		BoundStatement bs = bindParameters(ps, instanceId, filter, operator, attributes);
		return session.execute(bs).map(helper::get);
	}
	
	private Select addFilter(Select select, StoredMessageFilter filter)
	{
		if (filter.getStreamName() != null)
			select = FilterUtils.filterToWhere(filter.getStreamName().getOperation(), select.whereColumn(STREAM_NAME));
		
		if (filter.getDirection() != null)
			select = FilterUtils.filterToWhere(filter.getDirection().getOperation(), select.whereColumn(DIRECTION));
		
		if (filter.getIndex() != null)
		{
			ComparisonOperation operation = filter.getIndex().getOperation();
			//Overriding operation to include message's batch while selecting by query
			//While iterating through query results original operation will be used
			if (operation == ComparisonOperation.GREATER)
				operation = ComparisonOperation.GREATER_OR_EQUALS;
			else if (operation == ComparisonOperation.LESS)
				operation = ComparisonOperation.LESS_OR_EQUALS;
			select = FilterUtils.filterToWhere(operation, select.whereColumn(MESSAGE_INDEX));
		}
		
		if (filter.getTimestampFrom() != null)
		{
			ComparisonOperation op = filter.getTimestampFrom().getOperation();
			select = FilterUtils.filterToWhere(op, select.whereColumn(FIRST_MESSAGE_DATE));
			select = FilterUtils.filterToWhere(op, select.whereColumn(FIRST_MESSAGE_TIME));
		}
		
		if (filter.getTimestampTo() != null)
		{
			ComparisonOperation op = filter.getTimestampTo().getOperation();
			select = FilterUtils.filterToWhere(op, select.whereColumn(LAST_MESSAGE_DATE));
			select = FilterUtils.filterToWhere(op, select.whereColumn(LAST_MESSAGE_TIME));
		}
		
		if (filter.getLimit() > 0)
			select.limit(filter.getLimit());
		
		return select;
	}
	
	private BoundStatement bindParameters(PreparedStatement ps, UUID instanceId, StoredMessageFilter filter, MessageBatchOperator operator,
			Function<BoundStatementBuilder, BoundStatementBuilder> attributes)
	{
		BoundStatementBuilder builder = ps.boundStatementBuilder()
				.setUuid(INSTANCE_ID, instanceId);
		builder = attributes.apply(builder);
		if (filter != null)
			builder = bindFilterParameters(builder, filter, instanceId, operator, attributes);
		return builder.build();
	}
	
	private MessageBatchEntity getMessageBatch(StoredMessageFilter filter, MessageBatchOperator operator, UUID instanceId,
			Function<BoundStatementBuilder, BoundStatementBuilder> attributes)
	{
		if (filter.getStreamName() == null || filter.getDirection() == null)
		{
			//FIXME: throw exception to require stream name and direction to filter by message index
			//throw new CradleStorageException("Both streamName and direction are required when filtering by message index");
			return null;
		}
			
		return CassandraMessageUtils.getMessageBatch(new StoredMessageId(filter.getStreamName().getValue(), 
						filter.getDirection().getValue(), 
						filter.getIndex().getValue()),
				operator, instanceId, attributes);
	}
	
	private BoundStatementBuilder bindFilterParameters(BoundStatementBuilder builder, StoredMessageFilter filter, 
			UUID instanceId, MessageBatchOperator operator,
			Function<BoundStatementBuilder, BoundStatementBuilder> attributes)
	{
		if (filter.getStreamName() != null)
			builder = builder.setString(STREAM_NAME, filter.getStreamName().getValue());
		
		if (filter.getDirection() != null)
			builder = builder.setString(DIRECTION, filter.getDirection().getValue().getLabel());
		
		if (filter.getIndex() != null)
		{
			MessageBatchEntity batch = getMessageBatch(filter, operator, instanceId, attributes);
			builder = builder.setLong(MESSAGE_INDEX, batch != null ? batch.getMessageIndex() : filter.getIndex().getValue());
		}
		
		if (filter.getTimestampFrom() != null)
			builder = FilterUtils.bindTimestamp(filter.getTimestampFrom().getValue(), builder, FIRST_MESSAGE_DATE, FIRST_MESSAGE_TIME);
		
		if (filter.getTimestampTo() != null)
			builder = FilterUtils.bindTimestamp(filter.getTimestampTo().getValue(), builder, LAST_MESSAGE_DATE, LAST_MESSAGE_TIME);
		
		return builder;
	}
}

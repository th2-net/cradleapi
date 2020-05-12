/******************************************************************************
 * Copyright (c) 2009-2020, Exactpro Systems LLC
 * www.exactpro.com
 * Build Software to Test Software
 *
 * All rights reserved.
 * This is unpublished, licensed software, confidential and proprietary 
 * information which is the property of Exactpro Systems LLC or its licensors.
 ******************************************************************************/

package com.exactpro.cradle.cassandra.utils;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.selectFrom;
import static com.exactpro.cradle.cassandra.StorageConstants.*;

import java.util.UUID;

import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.exactpro.cradle.Direction;
import com.exactpro.cradle.filters.FilterByField;
import com.exactpro.cradle.messages.StoredMessageFilter;

public class CassandraMessageUtils
{
	public static Select prepareSelect(String keyspace, String tableName, UUID instanceId)
	{
		return selectFrom(keyspace, tableName)
				.all()
				.whereColumn(INSTANCE_ID).isEqualTo(literal(instanceId));
	}
	
	public static Select applyFilter(Select select, StoredMessageFilter filter)
	{
		if (filter.getStreamName() != null)
			select = FilterUtils.filterToWhere(filter.getStreamName(), select.whereColumn(STREAM_NAME));
		
		if (filter.getDirection() != null)
		{
			FilterByField<Direction> df = filter.getDirection();
			select = FilterUtils.filterToWhere(df.getValue().getLabel(), df.getOperation(), select.whereColumn(DIRECTION));
		}
		
		if (filter.getIndex() != null)
		{
			//TODO: not implemented. Probably, this should be not here because first of all we need to find a batch with messages to start filtering by index
		}
		
		if (filter.getTimestampFrom() != null)
		{
			select = FilterUtils.timestampFilterToWhere(filter.getTimestampFrom(), 
					select.whereColumn(FIRST_MESSAGE_DATE),
					select.whereColumn(FIRST_MESSAGE_TIME));
		}
		if (filter.getTimestampTo() != null)
		{
			select = FilterUtils.timestampFilterToWhere(filter.getTimestampTo(), 
					select.whereColumn(LAST_MESSAGE_DATE),
					select.whereColumn(LAST_MESSAGE_TIME));
		}
		
		if (filter.getLimit() > 0)
			select = select.limit(filter.getLimit());
		select = select.allowFiltering();
		return select;
	}
	
	public static Select prepareSelect(String keyspace, String tableName, UUID instanceId, StoredMessageFilter filter)
	{
		Select result = prepareSelect(keyspace, tableName, instanceId);
		if (filter == null)
			return result;
		
		return applyFilter(result, filter);
	}
}

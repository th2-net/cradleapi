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

import java.util.Iterator;
import java.util.UUID;
import java.util.function.Function;

import com.datastax.oss.driver.api.core.PagingIterable;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.exactpro.cradle.cassandra.dao.messages.MessageBatchEntity;
import com.exactpro.cradle.cassandra.dao.messages.MessageBatchOperator;
import com.exactpro.cradle.messages.StoredMessageBatch;
import com.exactpro.cradle.messages.StoredMessageId;

public class CassandraMessageUtils
{
	public static Select prepareSelect(String keyspace, String tableName, UUID instanceId)
	{
		return selectFrom(keyspace, tableName)
				.all()
				.whereColumn(INSTANCE_ID).isEqualTo(literal(instanceId));
	}
	
	public static MessageBatchEntity getMessageBatch(StoredMessageId id, MessageBatchOperator op, UUID instanceId,
			Function<BoundStatementBuilder, BoundStatementBuilder> readAttrs)
	{
		PagingIterable<MessageBatchEntity> batches = op.getMessageBatches(instanceId, 
						id.getStreamName(), 
						id.getDirection().getLabel(),
						id.getIndex()-StoredMessageBatch.MAX_MESSAGES_COUNT,
						id.getIndex(),
						readAttrs);
		if (batches == null)
			return null;
		
		MessageBatchEntity result = null;
		for (Iterator<MessageBatchEntity> it = batches.iterator(); it.hasNext(); )  //Getting last entity
			result = it.next();
		return result;
	}
}

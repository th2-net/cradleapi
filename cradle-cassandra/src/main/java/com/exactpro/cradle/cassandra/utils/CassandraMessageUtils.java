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
import com.exactpro.cradle.cassandra.dao.messages.DetailedMessageBatchEntity;
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
	
	public static DetailedMessageBatchEntity getMessageBatch(StoredMessageId id, MessageBatchOperator op, UUID instanceId,
			Function<BoundStatementBuilder, BoundStatementBuilder> readAttrs)
	{
		PagingIterable<DetailedMessageBatchEntity> batches = op.getMessageBatches(instanceId, 
						id.getStreamName(), 
						id.getDirection().getLabel(),
						id.getIndex()-StoredMessageBatch.MAX_MESSAGES_COUNT,
						id.getIndex(),
						readAttrs);
		if (batches == null)
			return null;
		
		DetailedMessageBatchEntity result = null;
		for (Iterator<DetailedMessageBatchEntity> it = batches.iterator(); it.hasNext(); )  //Getting last entity
			result = it.next();
		return result;
	}
}

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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import com.datastax.oss.driver.api.core.PagingIterable;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.exactpro.cradle.cassandra.CassandraSemaphore;
import com.exactpro.cradle.cassandra.dao.AsyncOperator;
import com.exactpro.cradle.cassandra.dao.messages.DetailedMessageBatchEntity;
import com.exactpro.cradle.cassandra.dao.messages.MessageBatchOperator;
import com.exactpro.cradle.filters.ComparisonOperation;
import com.exactpro.cradle.messages.StoredMessage;
import com.exactpro.cradle.messages.StoredMessageFilter;
import com.exactpro.cradle.messages.StoredMessageId;

public class CassandraMessageUtils
{
	public static Select prepareSelect(String keyspace, String tableName, UUID instanceId)
	{
		return selectFrom(keyspace, tableName)
				.all()
				.whereColumn(INSTANCE_ID).isEqualTo(literal(instanceId));
	}
	
	public static CompletableFuture<DetailedMessageBatchEntity> getMessageBatch(StoredMessageId id, MessageBatchOperator op, CassandraSemaphore semaphore,
			UUID instanceId, Function<BoundStatementBuilder, BoundStatementBuilder> readAttrs)
	{
		return new AsyncOperator<DetailedMessageBatchEntity>(semaphore)
				.getFuture(() -> op.getMessageBatch(instanceId,
							id.getStreamName(), 
							id.getDirection().getLabel(),
							id.getIndex(),
							readAttrs))
				.thenApply(batch ->{
					if (batch == null || batch.getLastMessageIndex() >= id.getIndex())
						return batch;						
					return null;
				});
	}
	
	public static long findLeftMessageIndex(DetailedMessageBatchEntity batch, StoredMessageFilter filter, UUID instanceId, 
			MessageBatchOperator op, Function<BoundStatementBuilder, BoundStatementBuilder> readAttrs) throws IOException
	{
		List<StoredMessage> batchMessages = new ArrayList<>(batch.toStoredMessages());
		int count = filter.getLimit();
		boolean inclusive = filter.getIndex().getOperation() == ComparisonOperation.LESS_OR_EQUALS;
		
		//Need to find out how many messages are on the left of current one in given batch.
		//Number of messages to iterate in other batches should be reduced  by this number
		long index = filter.getIndex().getValue();
		boolean found = false;
		for (ListIterator<StoredMessage> batchMessagesIt = batchMessages.listIterator(batchMessages.size()); batchMessagesIt.hasPrevious(); )
		{
			StoredMessage m = batchMessagesIt.previous();
			if (!found)
			{
				if (m.getIndex() == index)
				{
					if (inclusive)
						count--;
					found = true;
				}
				continue;
			}
			
			count--;
			//If left bound is in the same batch - returning index of this batch...
			if (count <= 0)
			{
				filter.setLeftBoundIndex(m.getIndex());
				return batch.getMessageIndex();
			}
		}
		
		//...else searching in previous batches, iterating through their messages from the end to find message index which is the left bound
		PagingIterable<DetailedMessageBatchEntity> otherBatches = op.getMessageBatchesReversed(instanceId, 
				batch.getStreamName(), 
				batch.getDirection(), 
				batch.getMessageIndex()-1	, readAttrs);
		if (otherBatches == null)
			return -1;
		
		for (Iterator<DetailedMessageBatchEntity> it = otherBatches.iterator(); it.hasNext(); )
		{
			DetailedMessageBatchEntity ob = it.next();
			if (ob.getMessageCount() <= count)  //Is needed message outside of this batch?
			{
				count -= ob.getMessageCount();
				if (count <= 0)  //If needed message is in the beginning of the batch
					return ob.getMessageIndex();
				continue;
			}
			
			List<StoredMessage> obMessages = new ArrayList<>(ob.toStoredMessages());
			for (ListIterator<StoredMessage> listIt = obMessages.listIterator(obMessages.size()); listIt.hasPrevious(); )
			{
				StoredMessage m = listIt.previous();
				count--;
				if (count <= 0)
				{
					filter.setLeftBoundIndex(m.getIndex());
					return ob.getMessageIndex();
				}
			}
		}
		
		return -1;
	}
}

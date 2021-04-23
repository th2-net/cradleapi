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

package com.exactpro.cradle.cassandra.amazon.dao.linkers;

import com.datastax.oss.driver.api.core.MappedAsyncPagingIterable;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.exactpro.cradle.cassandra.CassandraSemaphore;
import com.exactpro.cradle.cassandra.amazon.dao.AmazonOperators;
import com.exactpro.cradle.cassandra.amazon.dao.testevents.AmazonTestEventMessagesEntity;
import com.exactpro.cradle.cassandra.amazon.dao.testevents.AmazonTestEventMessagesOperator;
import com.exactpro.cradle.cassandra.dao.AsyncOperator;
import com.exactpro.cradle.cassandra.iterators.PagedIterator;
import com.exactpro.cradle.cassandra.linkers.CassandraTestEventsMessagesLinker;
import com.exactpro.cradle.messages.StoredMessageId;
import com.exactpro.cradle.testevents.StoredTestEventId;

import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

public class AmazonTestEventsMessagesLinker extends CassandraTestEventsMessagesLinker
{
	private AmazonTestEventMessagesOperator testEventsOperator;

	public AmazonTestEventsMessagesLinker(AmazonOperators ops, UUID instanceId,
			Function<BoundStatementBuilder, BoundStatementBuilder> readAttrs, CassandraSemaphore semaphore)
	{
		super(ops, instanceId, readAttrs, semaphore);
		testEventsOperator = ops.getAmazonTestEventMessagesOperator();
	}

	@Override
	public CompletableFuture<Collection<StoredMessageId>> getMessageIdsByTestEventIdAsync(StoredTestEventId eventId)
	{
		CompletableFuture<MappedAsyncPagingIterable<AmazonTestEventMessagesEntity>> future =
				new AsyncOperator<MappedAsyncPagingIterable<AmazonTestEventMessagesEntity>>(semaphore)
						.getFuture(() -> testEventsOperator.getMessages(instanceId, eventId.toString(), readAttrs));

		return future.thenCompose(rs -> getStoredMessageIdsAsync(new PagedIterator<>(rs)));
	}

	@Override
	public CompletableFuture<Boolean> isTestEventLinkedToMessagesAsync(StoredTestEventId eventId)
	{
		CompletableFuture<MappedAsyncPagingIterable<AmazonTestEventMessagesEntity>> future = 
				new AsyncOperator<MappedAsyncPagingIterable<AmazonTestEventMessagesEntity>>(semaphore)
				.getFuture(() -> testEventsOperator.getMessages(instanceId, eventId.toString(), readAttrs));

		return future.thenCompose(rs -> isThereLinkedMessagesAsync(new PagedIterator<>(rs)));
	}
}
	

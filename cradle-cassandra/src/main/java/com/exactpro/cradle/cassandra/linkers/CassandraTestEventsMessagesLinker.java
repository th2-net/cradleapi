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

package com.exactpro.cradle.cassandra.linkers;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Function;

import com.exactpro.cradle.cassandra.retries.SelectQueryExecutor;
import com.exactpro.cradle.messages.StoredMessageId;
import com.exactpro.cradle.testevents.StoredTestEventId;
import com.exactpro.cradle.testevents.TestEventsMessagesLinker;
import com.exactpro.cradle.utils.CradleIdException;
import com.datastax.oss.driver.api.core.MappedAsyncPagingIterable;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.exactpro.cradle.cassandra.CassandraSemaphore;
import com.exactpro.cradle.cassandra.dao.AsyncOperator;
import com.exactpro.cradle.cassandra.dao.messages.MessageTestEventEntity;
import com.exactpro.cradle.cassandra.dao.testevents.TestEventMessagesEntity;
import com.exactpro.cradle.cassandra.iterators.PagedIterator;
import com.exactpro.cradle.cassandra.retries.PagingSupplies;

public class CassandraTestEventsMessagesLinker implements TestEventsMessagesLinker
{
	private final LinkerSupplies supplies;
	private final UUID instanceId;
	private final Function<BoundStatementBuilder, BoundStatementBuilder> readAttrs;
	private final CassandraSemaphore semaphore;
	private final SelectQueryExecutor selectExec;
	private final PagingSupplies pagingSupplies;
	
	public CassandraTestEventsMessagesLinker(LinkerSupplies supplies, 
			UUID instanceId, Function<BoundStatementBuilder, BoundStatementBuilder> readAttrs, CassandraSemaphore semaphore,
			SelectQueryExecutor selectExec, PagingSupplies pagingSupplies)
	{
		this.supplies = supplies;
		this.instanceId = instanceId;
		this.readAttrs = readAttrs;
		this.semaphore = semaphore;
		this.selectExec = selectExec;
		this.pagingSupplies = pagingSupplies;
	}
	
	
	@Override
	public Collection<StoredTestEventId> getTestEventIdsByMessageId(StoredMessageId messageId) throws IOException
	{
		try
		{
			return getTestEventIdsByMessageIdAsync(messageId).get();
		}
		catch (Exception e)
		{
			throw new IOException("Error while getting IDs of test events linked to message "+messageId, e);
		}
	}
	
	@Override
	public CompletableFuture<Collection<StoredTestEventId>> getTestEventIdsByMessageIdAsync(StoredMessageId messageId)
	{
		String queryInfo = "get test events for messageId="+messageId;
		CompletableFuture<MappedAsyncPagingIterable<MessageTestEventEntity>> future = new AsyncOperator<MappedAsyncPagingIterable<MessageTestEventEntity>>(semaphore)
				.getFuture(() -> selectExec
						.executeMultiRowResultQuery(() -> supplies.getMessagesOperator().getTestEvents(instanceId, messageId.toString(), readAttrs),
								supplies.getMessageConverter(),
								queryInfo));
		
		return future.thenApplyAsync((rs) -> {
				PagedIterator<MessageTestEventEntity> it = new PagedIterator<>(rs, pagingSupplies, supplies.getMessageConverter(), queryInfo);
				Set<StoredTestEventId> ids = new HashSet<>();
				while (it.hasNext())
				{
					String eventId = it.next().getEventId();
					StoredTestEventId parsedId = new StoredTestEventId(eventId);
					ids.add(parsedId);
				}
				
				if (ids.isEmpty())
					ids = null;
				
				return ids;
			});
	}
	
	
	@Override
	public Collection<StoredMessageId> getMessageIdsByTestEventId(StoredTestEventId eventId) throws IOException
	{
		try
		{
			return getMessageIdsByTestEventIdAsync(eventId).get();
		}
		catch (Exception e)
		{
			throw new IOException("Error while getting IDs of messages linked to test event "+eventId, e);
		}
	}
	
	@Override
	public CompletableFuture<Collection<StoredMessageId>> getMessageIdsByTestEventIdAsync(StoredTestEventId eventId)
	{
		String queryInfo = "get messages for eventId="+eventId;
		CompletableFuture<MappedAsyncPagingIterable<TestEventMessagesEntity>> future = new AsyncOperator<MappedAsyncPagingIterable<TestEventMessagesEntity>>(semaphore)
				.getFuture(() -> selectExec
						.executeMultiRowResultQuery(() -> supplies.getTestEventsOperator().getMessages(instanceId, eventId.toString(), readAttrs),
								supplies.getTestEventConverter(),
								queryInfo));
		
		return future.thenApplyAsync((rs) -> {
				PagedIterator<TestEventMessagesEntity> it = new PagedIterator<>(rs, pagingSupplies, supplies.getTestEventConverter(), queryInfo);
				Set<StoredMessageId> ids = new HashSet<>();
				while (it.hasNext())
				{
					Set<String> currentMessageIds = it.next().getMessageIds();
					if (currentMessageIds == null)
						continue;
					
					for (String cid : currentMessageIds)
					{
						try
						{
							StoredMessageId parsedId = StoredMessageId.fromString(cid);
							ids.add(parsedId);
						}
						catch (CradleIdException e)
						{
							throw new CompletionException("Could not parse message ID from '"+cid+"'", e);
						}
					}
				}
				
				if (ids.isEmpty())
					ids = null;
				
				return ids;
			});
	}
	
	
	@Override
	public boolean isTestEventLinkedToMessages(StoredTestEventId eventId) throws IOException
	{
		try
		{
			return isTestEventLinkedToMessagesAsync(eventId).get();
		}
		catch (Exception e)
		{
			throw new IOException("Error while getting if test event "+eventId+" has messages linked to it", e);
		}
	}
	
	@Override
	public CompletableFuture<Boolean> isTestEventLinkedToMessagesAsync(StoredTestEventId eventId)
	{
		String queryInfo = "get messages for eventId="+eventId+" to check links";
		CompletableFuture<MappedAsyncPagingIterable<TestEventMessagesEntity>> future = new AsyncOperator<MappedAsyncPagingIterable<TestEventMessagesEntity>>(semaphore)
				.getFuture(() -> selectExec
						.executeMultiRowResultQuery(() -> supplies.getTestEventsOperator().getMessages(instanceId, eventId.toString(), readAttrs),
								supplies.getTestEventConverter(),
								queryInfo));
		
		return future.thenApplyAsync((rs) -> {
				PagedIterator<TestEventMessagesEntity> it = new PagedIterator<>(rs, pagingSupplies, supplies.getTestEventConverter(), queryInfo);
				boolean result = false;
				while (it.hasNext())
				{
					Collection<String> ids = it.next().getMessageIds();
					if (ids != null && !ids.isEmpty())
					{
						result = true;
						break;
					}
				}
				return result;
			});
	}
	
	@Override
	public boolean isMessageLinkedToTestEvents(StoredMessageId messageId) throws IOException
	{
		try
		{
			return isMessageLinkedToTestEventsAsync(messageId).get();
		}
		catch (Exception e)
		{
			throw new IOException("Error while getting if message "+messageId+" has test events linked to it", e);
		}
	}
	
	@Override
	public CompletableFuture<Boolean> isMessageLinkedToTestEventsAsync(StoredMessageId messageId)
	{
		String queryInfo = "get test events for messageId="+messageId+" to check links";
		CompletableFuture<MappedAsyncPagingIterable<MessageTestEventEntity>> future = new AsyncOperator<MappedAsyncPagingIterable<MessageTestEventEntity>>(semaphore)
				.getFuture(() -> selectExec
						.executeMultiRowResultQuery(() -> supplies.getMessagesOperator().getTestEvents(instanceId, messageId.toString(), readAttrs),
								supplies.getMessageConverter(),
								queryInfo));
		
		return future.thenApply((rs) -> {
				PagedIterator<MessageTestEventEntity> it = new PagedIterator<>(rs, pagingSupplies, supplies.getMessageConverter(), queryInfo);
				return it.hasNext();
			});
	}
}

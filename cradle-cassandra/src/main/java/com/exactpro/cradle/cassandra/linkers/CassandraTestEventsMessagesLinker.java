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
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

import com.exactpro.cradle.messages.StoredMessageId;
import com.exactpro.cradle.testevents.StoredTestEventId;
import com.exactpro.cradle.testevents.TestEventsMessagesLinker;
import com.exactpro.cradle.utils.CradleIdException;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.exactpro.cradle.cassandra.dao.messages.MessageTestEventEntity;
import com.exactpro.cradle.cassandra.dao.messages.MessageTestEventOperator;
import com.exactpro.cradle.cassandra.dao.testevents.TestEventMessagesEntity;
import com.exactpro.cradle.cassandra.dao.testevents.TestEventMessagesOperator;
import com.exactpro.cradle.cassandra.iterators.PagedIterator;
import com.exactpro.cradle.cassandra.retry.AsyncExecutor;
import com.exactpro.cradle.cassandra.retry.SyncExecutor;

public class CassandraTestEventsMessagesLinker implements TestEventsMessagesLinker
{
	private final TestEventMessagesOperator testEventsOperator;
	private final MessageTestEventOperator messagesOperator;
	private final UUID instanceId;
	private final SyncExecutor syncExecutor;
	private final AsyncExecutor asyncExecutor;
	private final ExecutorService composingService;
	private final Function<BoundStatementBuilder, BoundStatementBuilder> readAttrs;
	
	public CassandraTestEventsMessagesLinker(TestEventMessagesOperator testEventsOperator, MessageTestEventOperator messagesOperator, 
			UUID instanceId, 
			SyncExecutor syncExecutor, AsyncExecutor asyncExecutor, ExecutorService composingService, 
			Function<BoundStatementBuilder, BoundStatementBuilder> readAttrs)
	{
		this.testEventsOperator = testEventsOperator;
		this.messagesOperator = messagesOperator;
		this.instanceId = instanceId;
		this.syncExecutor = syncExecutor;
		this.asyncExecutor = asyncExecutor;
		this.composingService = composingService;
		this.readAttrs = readAttrs;
	}
	
	
	@Override
	public Collection<StoredTestEventId> getTestEventIdsByMessageId(StoredMessageId messageId) throws IOException
	{
		try
		{
			return syncExecutor.submit("get event IDs linked to message "+messageId, 
					() -> readEventIds(messageId));
		}
		catch (Exception e)
		{
			throw new IOException("Error while getting IDs of test events linked to message "+messageId, e);
		}
	}
	
	@Override
	public CompletableFuture<Collection<StoredTestEventId>> getTestEventIdsByMessageIdAsync(StoredMessageId messageId)
	{
		return asyncExecutor.submit("get event IDs linked to message "+messageId, 
				() -> readEventIds(messageId));
	}
	
	
	@Override
	public Collection<StoredMessageId> getMessageIdsByTestEventId(StoredTestEventId eventId) throws IOException
	{
		try
		{
			return syncExecutor.submit("get message IDs linked to event "+eventId, 
					() -> readMessageIds(eventId));
		}
		catch (Exception e)
		{
			throw new IOException("Error while getting IDs of messages linked to test event "+eventId, e);
		}
	}
	
	@Override
	public CompletableFuture<Collection<StoredMessageId>> getMessageIdsByTestEventIdAsync(StoredTestEventId eventId)
	{
		return asyncExecutor.submit("get message IDs linked to event "+eventId, 
				() -> readMessageIds(eventId));
	}
	
	
	@Override
	public boolean isTestEventLinkedToMessages(StoredTestEventId eventId) throws IOException
	{
		try
		{
			return syncExecutor.submit("get if event "+eventId+" is linked to messages", 
					() -> checkIfMessagesLinked(eventId));
		}
		catch (Exception e)
		{
			throw new IOException("Error while getting if test event "+eventId+" has messages linked to it", e);
		}
	}
	
	@Override
	public CompletableFuture<Boolean> isTestEventLinkedToMessagesAsync(StoredTestEventId eventId)
	{
		return asyncExecutor.submit("get if event "+eventId+" is linked to messages", 
				() -> checkIfMessagesLinked(eventId));
	}
	
	@Override
	public boolean isMessageLinkedToTestEvents(StoredMessageId messageId) throws IOException
	{
		try
		{
			return syncExecutor.submit("get if message "+messageId+" is linked to events", 
					() -> checkIfEventsLinked(messageId));
		}
		catch (Exception e)
		{
			throw new IOException("Error while getting if message "+messageId+" has test events linked to it", e);
		}
	}
	
	@Override
	public CompletableFuture<Boolean> isMessageLinkedToTestEventsAsync(StoredMessageId messageId)
	{
		return asyncExecutor.submit("get if message "+messageId+" is linked to events", 
				() -> checkIfEventsLinked(messageId));
	}
	
	
	private CompletableFuture<Collection<StoredTestEventId>> readEventIds(StoredMessageId messageId)
	{
		return messagesOperator.getTestEvents(instanceId, messageId.toString(), readAttrs)
				.thenApplyAsync((rs) -> {
					PagedIterator<MessageTestEventEntity> it = new PagedIterator<>(rs);
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
				}, composingService);
	}
	
	private CompletableFuture<Collection<StoredMessageId>> readMessageIds(StoredTestEventId eventId)
	{
		return testEventsOperator.getMessages(instanceId, eventId.toString(), readAttrs)
				.thenApplyAsync((rs) -> {
					PagedIterator<TestEventMessagesEntity> it = new PagedIterator<>(rs);
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
				}, composingService);
	}
	
	private CompletableFuture<Boolean> checkIfMessagesLinked(StoredTestEventId eventId)
	{
		return testEventsOperator.getMessages(instanceId, eventId.toString(), readAttrs)
				.thenApply((rs) -> {
					PagedIterator<TestEventMessagesEntity> it = new PagedIterator<>(rs);
					while (it.hasNext())
					{
						Collection<String> ids = it.next().getMessageIds();
						if (ids != null && !ids.isEmpty())
							return true;
					}
					return false;
				});
	}
	
	private CompletableFuture<Boolean> checkIfEventsLinked(StoredMessageId messageId)
	{
		return messagesOperator.getTestEvents(instanceId, messageId.toString(), readAttrs)
				.thenApply((rs) -> new PagedIterator<>(rs).hasNext());
	}
}

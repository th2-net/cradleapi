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

package com.exactpro.cradle.cassandra.linkers;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.selectFrom;
import static com.exactpro.cradle.cassandra.StorageConstants.INSTANCE_ID;
import static com.exactpro.cradle.cassandra.StorageConstants.MESSAGE_IDS;
import static com.exactpro.cradle.cassandra.StorageConstants.MESSAGE_ID;
import static com.exactpro.cradle.cassandra.StorageConstants.TEST_EVENT_ID;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.UUID;

import com.exactpro.cradle.messages.StoredMessageId;
import com.exactpro.cradle.testevents.StoredTestEventId;
import com.exactpro.cradle.testevents.TestEventsMessagesLinker;
import com.exactpro.cradle.utils.CradleIdException;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.exactpro.cradle.cassandra.utils.QueryExecutor;

public class CassandraTestEventsMessagesLinker implements TestEventsMessagesLinker
{
	private final QueryExecutor exec;
	private final String keyspace,
			testEventsMessagesTable,
			messagesTestEventsTable;
	private final UUID instanceId;
	
	public CassandraTestEventsMessagesLinker(QueryExecutor exec, String keyspace, 
			String testEventsMessagesTable, String messagesTestEventsTable, UUID instanceId)
	{
		this.exec = exec;
		this.keyspace = keyspace;
		this.testEventsMessagesTable = testEventsMessagesTable;
		this.messagesTestEventsTable = messagesTestEventsTable;
		this.instanceId = instanceId;
	}
	
	
	@Override
	public Collection<StoredTestEventId> getTestEventIdsByMessageId(StoredMessageId messageId) throws IOException
	{
		Select selectFrom = prepareTestEventsOfMessageQuery(messageId.toString());
		
		Iterator<Row> resultIterator = exec.executeQuery(selectFrom.asCql(), false).iterator();
		Set<StoredTestEventId> ids = new HashSet<>();
		while (resultIterator.hasNext())
		{
			String eventId = resultIterator.next().get(TEST_EVENT_ID, GenericType.STRING);
			StoredTestEventId parsedId = new StoredTestEventId(eventId);
			ids.add(parsedId);
		}
		if (ids.isEmpty())
			return null;
		
		return ids;
	}
	
	@Override
	public Collection<StoredMessageId> getMessageIdsByTestEventId(StoredTestEventId eventId) throws IOException
	{
		Select selectFrom = prepareMessagesOfTestEventQuery(eventId.toString());
		
		Iterator<Row> resultIterator = exec.executeQuery(selectFrom.asCql(), false).iterator();
		Set<StoredMessageId> ids = new HashSet<>();
		while (resultIterator.hasNext())
		{
			Set<String> currentMessageIds = resultIterator.next().get(MESSAGE_IDS,
					GenericType.setOf(GenericType.STRING));
			if (currentMessageIds != null)
			{
				for (String cid : currentMessageIds)
				{
					try
					{
						StoredMessageId parsedId = StoredMessageId.fromString(cid);
						ids.add(parsedId);
					}
					catch (CradleIdException e)
					{
						throw new IOException("Could not parse message ID from '"+cid+"'", e);
					}
				}
			}
		}
		if (ids.isEmpty())
			return null;
		
		return ids;
	}
	
	@Override
	public boolean isTestEventLinkedToMessages(StoredTestEventId eventId) throws IOException
	{
		String id = eventId.toString();
		Select select = prepareMessagesOfTestEventQuery(id);
		return isLinked(select);
	}
	
	@Override
	public boolean isMessageLinkedToTestEvents(StoredMessageId messageId) throws IOException
	{
		String id = messageId.toString();
		Select select = prepareTestEventsOfMessageQuery(id);
		return isLinked(select);
	}
	
	
	
	private Select prepareMessagesOfTestEventQuery(String eventId)
	{
		return selectFrom(keyspace, testEventsMessagesTable)
				.column(MESSAGE_IDS)
				.whereColumn(INSTANCE_ID).isEqualTo(literal(instanceId))
				.whereColumn(TEST_EVENT_ID).isEqualTo(literal(eventId));
	}
	
	private Select prepareTestEventsOfMessageQuery(String messageId)
	{
		return selectFrom(keyspace, messagesTestEventsTable)
				.column(TEST_EVENT_ID)
				.whereColumn(INSTANCE_ID).isEqualTo(literal(instanceId))
				.whereColumn(MESSAGE_ID).isEqualTo(literal(messageId));
	}
	
	private boolean isLinked(Select select) throws IOException
	{
		Select selectFrom = select.limit(1);
		return exec.executeQuery(selectFrom.asCql(), false).one() != null;
	}
}
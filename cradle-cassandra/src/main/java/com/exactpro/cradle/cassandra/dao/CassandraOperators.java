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

package com.exactpro.cradle.cassandra.dao;

import com.exactpro.cradle.cassandra.CassandraStorageSettings;
import com.exactpro.cradle.cassandra.dao.messages.MessageBatchOperator;
import com.exactpro.cradle.cassandra.dao.messages.MessageTestEventOperator;
import com.exactpro.cradle.cassandra.dao.messages.StreamsOperator;
import com.exactpro.cradle.cassandra.dao.messages.TimeMessageOperator;
import com.exactpro.cradle.cassandra.dao.testevents.*;

public class CassandraOperators
{
	private MessageBatchOperator messageBatchOperator, 
			processedMessageBatchOperator;
	private StreamsOperator streamsOperator;
	private TimeMessageOperator timeMessageOperator;
	private TestEventOperator testEventOperator;
	private RootTestEventOperator rootTestEventOperator;
	private RootTestEventDatesOperator rootTestEventDatesOperator;
	private TestEventChildrenOperator testEventChildrenOperator;
	private TimeTestEventOperator timeTestEventOperator;
	private TestEventChildrenDatesOperator testEventChildrenDatesOperator;
	private TestEventMessagesOperator testEventMessagesOperator;
	private MessageTestEventOperator messageTestEventOperator;
	
	public CassandraOperators(CassandraDataMapper dataMapper, CassandraStorageSettings settings)
	{
		initMessageBatchOperator(dataMapper, settings);
		initStreamsOperator(dataMapper, settings);
		initProcessedMessageBatchOperator(dataMapper, settings);
		initTimeMessageOperator(dataMapper, settings);
		initTestEventOperator(dataMapper, settings);
		initTimeTestEventOperator(dataMapper, settings);
		initRootTestEventOperator(dataMapper, settings);
		initRootTestEventDatesOperator(dataMapper, settings);
		initTestEventChildrenOperator(dataMapper, settings);
		initTestEventChildrenDatesOperator(dataMapper, settings);
		initTestEventMessagesOperator(dataMapper, settings);
		initMessageTestEventOperator(dataMapper, settings);
	}

	protected void initMessageBatchOperator(CassandraDataMapper dataMapper, CassandraStorageSettings settings)
	{
		messageBatchOperator = dataMapper.messageBatchOperator(settings.getKeyspace(), settings.getMessagesTableName());
	}

	protected void initStreamsOperator(CassandraDataMapper dataMapper, CassandraStorageSettings settings)
	{
		streamsOperator = dataMapper.streamsOperator(settings.getKeyspace(), settings.getMessagesTableName());
	}

	protected void initProcessedMessageBatchOperator(CassandraDataMapper dataMapper, CassandraStorageSettings settings)
	{
		processedMessageBatchOperator = dataMapper.messageBatchOperator(settings.getKeyspace(), settings.getProcessedMessagesTableName());
	}

	protected void initTimeMessageOperator(CassandraDataMapper dataMapper, CassandraStorageSettings settings)
	{
		timeMessageOperator = dataMapper.timeMessageOperator(settings.getKeyspace(), settings.getTimeMessagesTableName());
	}
	
	protected void initTestEventOperator(CassandraDataMapper dataMapper, CassandraStorageSettings settings)
	{
		testEventOperator = dataMapper.testEventOperator(settings.getKeyspace(), settings.getTestEventsTableName());
	}
	
	protected void initTimeTestEventOperator(CassandraDataMapper dataMapper, CassandraStorageSettings settings)
	{
		timeTestEventOperator = dataMapper.timeTestEventOperator(settings.getKeyspace(), settings.getTimeTestEventsTableName());
	}
	
	protected void initRootTestEventOperator(CassandraDataMapper dataMapper, CassandraStorageSettings settings)
	{
		rootTestEventOperator = dataMapper.rootTestEventOperator(settings.getKeyspace(), settings.getRootTestEventsTableName());
	}

	protected void initRootTestEventDatesOperator(CassandraDataMapper dataMapper, CassandraStorageSettings settings)
	{
		rootTestEventDatesOperator = dataMapper.rootTestEventDatesOperator(settings.getKeyspace(), settings.getRootTestEventsTableName());
	}
	
	protected void initTestEventChildrenOperator(CassandraDataMapper dataMapper, CassandraStorageSettings settings)
	{
		testEventChildrenOperator = dataMapper.testEventChildrenOperator(settings.getKeyspace(), settings.getTestEventsChildrenTableName());
	}
	
	protected void initTestEventChildrenDatesOperator(CassandraDataMapper dataMapper, CassandraStorageSettings settings)
	{
		testEventChildrenDatesOperator = dataMapper.testEventChildrenDatesOperator(settings.getKeyspace(), settings.getTestEventsChildrenDatesTableName());
	}
	
	protected void initTestEventMessagesOperator(CassandraDataMapper dataMapper, CassandraStorageSettings settings)
	{
		testEventMessagesOperator = dataMapper.testEventMessagesOperator(settings.getKeyspace(), settings.getTestEventsMessagesTableName());	
	}
	
	protected void initMessageTestEventOperator(CassandraDataMapper dataMapper, CassandraStorageSettings settings)
	{
		messageTestEventOperator = dataMapper.messageTestEventOperator(settings.getKeyspace(), settings.getMessagesTestEventsTableName());
	}
	
	
	public MessageBatchOperator getMessageBatchOperator()
	{
		return messageBatchOperator;
	}
	
	public MessageBatchOperator getProcessedMessageBatchOperator()
	{
		return processedMessageBatchOperator;
	}
	
	public TimeMessageOperator getTimeMessageOperator()
	{
		return timeMessageOperator;
	}
	
	public TestEventOperator getTestEventOperator()
	{
		return testEventOperator;
	}
	
	public RootTestEventOperator getRootTestEventOperator()
	{
		return rootTestEventOperator;
	}

	public TestEventChildrenOperator getTestEventChildrenOperator()
	{
		return testEventChildrenOperator;
	}
	
	public TimeTestEventOperator getTimeTestEventOperator()
	{
		return timeTestEventOperator;
	}
	
	public TestEventChildrenDatesOperator getTestEventChildrenDatesOperator()
	{
		return testEventChildrenDatesOperator;
	}
	
	public TestEventMessagesOperator getTestEventMessagesOperator()
	{
		return testEventMessagesOperator;
	}
	
	public MessageTestEventOperator getMessageTestEventOperator()
	{
		return messageTestEventOperator;
	}

	public StreamsOperator getStreamsOperator()
	{
		return streamsOperator;
	}

	public RootTestEventDatesOperator getRootTestEventDatesOperator()
	{
		return rootTestEventDatesOperator;
	}
}

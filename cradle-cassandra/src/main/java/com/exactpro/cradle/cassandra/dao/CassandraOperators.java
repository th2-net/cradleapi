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

package com.exactpro.cradle.cassandra.dao;

import com.exactpro.cradle.cassandra.CassandraStorageSettings;
import com.exactpro.cradle.cassandra.dao.intervals.IntervalOperator;
import com.exactpro.cradle.cassandra.dao.intervals.TimeIntervalOperator;
import com.exactpro.cradle.cassandra.dao.messages.MessageBatchOperator;
import com.exactpro.cradle.cassandra.dao.messages.MessageTestEventOperator;
import com.exactpro.cradle.cassandra.dao.messages.TimeMessageOperator;
import com.exactpro.cradle.cassandra.dao.testevents.RootTestEventOperator;
import com.exactpro.cradle.cassandra.dao.testevents.TestEventChildrenDatesOperator;
import com.exactpro.cradle.cassandra.dao.testevents.TestEventChildrenOperator;
import com.exactpro.cradle.cassandra.dao.testevents.TestEventMessagesOperator;
import com.exactpro.cradle.cassandra.dao.testevents.TestEventOperator;
import com.exactpro.cradle.cassandra.dao.testevents.TimeTestEventOperator;

public class CassandraOperators
{
	private MessageBatchOperator messageBatchOperator, 
			processedMessageBatchOperator;
	private TimeMessageOperator timeMessageOperator;
	private TestEventOperator testEventOperator;
	private RootTestEventOperator rootTestEventOperator;
	private TestEventChildrenOperator testEventChildrenOperator;
	private TimeTestEventOperator timeTestEventOperator;
	private TestEventChildrenDatesOperator testEventChildrenDatesOperator;
	private TestEventMessagesOperator testEventMessagesOperator;
	private MessageTestEventOperator messageTestEventOperator;
	private TimeIntervalOperator timeIntervalOperator;
	private IntervalOperator intervalOperator;

	public CassandraOperators(CassandraDataMapper dataMapper, CassandraStorageSettings settings)
	{
		messageBatchOperator = dataMapper.messageBatchOperator(settings.getKeyspace(), settings.getMessagesTableName());
		processedMessageBatchOperator = dataMapper.messageBatchOperator(settings.getKeyspace(), settings.getProcessedMessagesTableName());
		timeMessageOperator = dataMapper.timeMessageOperator(settings.getKeyspace(), settings.getTimeMessagesTableName());
		testEventOperator = dataMapper.testEventOperator(settings.getKeyspace(), settings.getTestEventsTableName());
		timeTestEventOperator = dataMapper.timeTestEventOperator(settings.getKeyspace(), settings.getTimeTestEventsTableName());
		rootTestEventOperator = dataMapper.rootTestEventOperator(settings.getKeyspace(), settings.getRootTestEventsTableName());
		testEventChildrenOperator = dataMapper.testEventChildrenOperator(settings.getKeyspace(), settings.getTestEventsChildrenTableName());
		testEventChildrenDatesOperator = dataMapper.testEventChildrenDatesOperator(settings.getKeyspace(), settings.getTestEventsChildrenDatesTableName());
		testEventMessagesOperator = dataMapper.testEventMessagesOperator(settings.getKeyspace(), settings.getTestEventsMessagesTableName());
		messageTestEventOperator = dataMapper.messageTestEventOperator(settings.getKeyspace(), settings.getMessagesTestEventsTableName());
		intervalOperator = dataMapper.intervalOperator(settings.getKeyspace(), settings.getIntervalsTableName());
		timeIntervalOperator = dataMapper.timeIntervalOperator(settings.getKeyspace(), settings.getTimeIntervalsTableName());
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

	public TimeIntervalOperator getTimeIntervalOperator() { return timeIntervalOperator; }

	public IntervalOperator getIntervalOperator() { return intervalOperator; }
}

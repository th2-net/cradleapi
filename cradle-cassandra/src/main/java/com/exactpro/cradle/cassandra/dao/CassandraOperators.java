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
import com.exactpro.cradle.cassandra.dao.intervals.IntervalOperator;
import com.exactpro.cradle.cassandra.dao.intervals.converters.IntervalConverter;
import com.exactpro.cradle.cassandra.dao.messages.MessageBatchOperator;
import com.exactpro.cradle.cassandra.dao.messages.MessageTestEventConverter;
import com.exactpro.cradle.cassandra.dao.messages.MessageTestEventOperator;
import com.exactpro.cradle.cassandra.dao.messages.TimeMessageOperator;
import com.exactpro.cradle.cassandra.dao.messages.converters.DetailedMessageBatchConverter;
import com.exactpro.cradle.cassandra.dao.testevents.TestEventChildrenDatesOperator;
import com.exactpro.cradle.cassandra.dao.testevents.TestEventMessagesOperator;
import com.exactpro.cradle.cassandra.dao.testevents.TestEventOperator;
import com.exactpro.cradle.cassandra.dao.testevents.TimeTestEventOperator;
import com.exactpro.cradle.cassandra.dao.testevents.converters.TestEventConverter;
import com.exactpro.cradle.cassandra.dao.testevents.converters.TestEventMessagesConverter;

public class CassandraOperators
{
	private final MessageBatchOperator messageBatchOperator, 
			processedMessageBatchOperator;
	private final TimeMessageOperator timeMessageOperator;
	private final TestEventOperator testEventOperator;
	private final TimeTestEventOperator timeTestEventOperator;
	private final TestEventMessagesOperator testEventMessagesOperator;
	private final TestEventChildrenDatesOperator testEventChildrenDatesOperator;
	private final MessageTestEventOperator messageTestEventOperator;
	private final IntervalOperator intervalOperator;
	private final DetailedMessageBatchConverter messageBatchConverter;
	private final TestEventConverter testEventConverter;
	private final TestEventMessagesConverter testEventMessagesConverter;
	private final MessageTestEventConverter messageTestEventConverter;
	private final IntervalConverter intervalConverter;

	public CassandraOperators(CassandraDataMapper dataMapper, CassandraStorageSettings settings)
	{
		messageBatchOperator = dataMapper.messageBatchOperator(settings.getKeyspace(), settings.getMessagesTableName());
		processedMessageBatchOperator = dataMapper.messageBatchOperator(settings.getKeyspace(), settings.getProcessedMessagesTableName());
		timeMessageOperator = dataMapper.timeMessageOperator(settings.getKeyspace(), settings.getTimeMessagesTableName());
		testEventOperator = dataMapper.testEventOperator(settings.getKeyspace(), settings.getTestEventsTableName());
		timeTestEventOperator = dataMapper.timeTestEventOperator(settings.getKeyspace(), settings.getTimeTestEventsTableName());
		testEventChildrenDatesOperator = dataMapper.testEventChildrenDatesOperator(settings.getKeyspace(), settings.getTestEventsChildrenDatesTableName());
		testEventMessagesOperator = dataMapper.testEventMessagesOperator(settings.getKeyspace(), settings.getTestEventsMessagesTableName());
		messageTestEventOperator = dataMapper.messageTestEventOperator(settings.getKeyspace(), settings.getMessagesTestEventsTableName());
		intervalOperator = dataMapper.intervalOperator(settings.getKeyspace(), settings.getIntervalsTableName());
		messageBatchConverter = dataMapper.detailedMessageBatchConverter();
		testEventConverter = dataMapper.testEventConverter();
		testEventMessagesConverter = dataMapper.testEventMessagesConverter();
		messageTestEventConverter = dataMapper.messageTestEventConverter();
		intervalConverter = dataMapper.intervalConverter();
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

	public IntervalOperator getIntervalOperator() { return intervalOperator; }
	
	
	public DetailedMessageBatchConverter getMessageBatchConverter()
	{
		return messageBatchConverter;
	}
	
	public TestEventConverter getTestEventConverter()
	{
		return testEventConverter;
	}
	
	public TestEventMessagesConverter getTestEventMessagesConverter()
	{
		return testEventMessagesConverter;
	}
	
	public MessageTestEventConverter getMessageTestEventConverter()
	{
		return messageTestEventConverter;
	}
	
	public IntervalConverter getIntervalConverter()
	{
		return intervalConverter;
	}
}

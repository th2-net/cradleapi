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
import com.exactpro.cradle.cassandra.dao.intervals.converters.DateTimeEventEntityConverter;
import com.exactpro.cradle.cassandra.dao.intervals.converters.IntervalConverter;
import com.exactpro.cradle.cassandra.dao.messages.GroupedMessageBatchOperator;
import com.exactpro.cradle.cassandra.dao.messages.MessageBatchOperator;
import com.exactpro.cradle.cassandra.dao.messages.TimeMessageOperator;
import com.exactpro.cradle.cassandra.dao.messages.converters.DetailedMessageBatchConverter;
import com.exactpro.cradle.cassandra.dao.messages.converters.GroupedMessageBatchConverter;
import com.exactpro.cradle.cassandra.dao.messages.converters.TimeMessageConverter;
import com.exactpro.cradle.cassandra.dao.testevents.TestEventChildrenDatesOperator;
import com.exactpro.cradle.cassandra.dao.testevents.TestEventOperator;
import com.exactpro.cradle.cassandra.dao.testevents.TimeTestEventOperator;
import com.exactpro.cradle.cassandra.dao.testevents.converters.DateEventEntityConverter;
import com.exactpro.cradle.cassandra.dao.testevents.converters.DetailedTestEventConverter;
import com.exactpro.cradle.cassandra.dao.testevents.converters.TestEventConverter;
import com.exactpro.cradle.cassandra.dao.testevents.converters.TestEventMetadataConverter;

public class CassandraOperators
{
	private final MessageBatchOperator messageBatchOperator, 
			processedMessageBatchOperator;
	private final GroupedMessageBatchOperator groupedMessageBatchOperator;
	private final TimeMessageOperator timeMessageOperator;
	private final TestEventOperator testEventOperator;
	private final TimeTestEventOperator timeTestEventOperator;
	private final TestEventChildrenDatesOperator testEventChildrenDatesOperator;
	private final IntervalOperator intervalOperator;
	private final DetailedMessageBatchConverter messageBatchConverter;
	private final GroupedMessageBatchConverter groupedMessageBatchConverter;
	private final TestEventConverter testEventConverter;
	private final DetailedTestEventConverter detailedTestEventConverter;
	private final TestEventMetadataConverter testEventMetadataConverter;
	private final IntervalConverter intervalConverter;
	private final TimeMessageConverter timeMessageConverter;
	private final DateTimeEventEntityConverter dateTimeEventEntityConverter;
	private final DateEventEntityConverter dateEventEntityConverter;

	public CassandraOperators(CassandraDataMapper dataMapper, CassandraStorageSettings settings)
	{
		messageBatchOperator = dataMapper.messageBatchOperator(settings.getKeyspace(), settings.getMessagesTableName());
		groupedMessageBatchOperator = dataMapper.groupedMessageBatchOperator(settings.getKeyspace(), settings.getGroupedMessagesTableName());
		processedMessageBatchOperator = dataMapper.messageBatchOperator(settings.getKeyspace(), settings.getProcessedMessagesTableName());
		timeMessageOperator = dataMapper.timeMessageOperator(settings.getKeyspace(), settings.getTimeMessagesTableName());
		testEventOperator = dataMapper.testEventOperator(settings.getKeyspace(), settings.getTestEventsTableName());
		timeTestEventOperator = dataMapper.timeTestEventOperator(settings.getKeyspace(), settings.getTimeTestEventsTableName());
		testEventChildrenDatesOperator = dataMapper.testEventChildrenDatesOperator(settings.getKeyspace(), settings.getTestEventsChildrenDatesTableName());
		intervalOperator = dataMapper.intervalOperator(settings.getKeyspace(), settings.getIntervalsTableName());

		messageBatchConverter = dataMapper.detailedMessageBatchConverter();
		groupedMessageBatchConverter = dataMapper.groupedMessageBatchConverter();
		testEventConverter = dataMapper.testEventConverter();
		detailedTestEventConverter = dataMapper.detailedTestEventConverter();
		testEventMetadataConverter = dataMapper.testEventMetadataConverter();
		intervalConverter = dataMapper.intervalConverter();
		timeMessageConverter = dataMapper.timeMessageConverter();
		dateTimeEventEntityConverter = dataMapper.dateTimeEventEntityConverter();
		dateEventEntityConverter = dataMapper.dateEventEntityConverter();
	}

	public MessageBatchOperator getMessageBatchOperator()
	{
		return messageBatchOperator;
	}
	
	public GroupedMessageBatchOperator getGroupedMessageBatchOperator()
	{
		return groupedMessageBatchOperator;
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

	public IntervalOperator getIntervalOperator() { return intervalOperator; }
	
	
	public DetailedMessageBatchConverter getMessageBatchConverter()
	{
		return messageBatchConverter;
	}
	
	public GroupedMessageBatchConverter getGroupedMessageBatchConverter()
	{
		return groupedMessageBatchConverter;
	}
	
	public TestEventConverter getTestEventConverter()
	{
		return testEventConverter;
	}

	public TestEventMetadataConverter getTestEventMetadataConverter()
	{
		return testEventMetadataConverter;
	}

	public IntervalConverter getIntervalConverter()
	{
		return intervalConverter;
	}

	public TimeMessageConverter getTimeMessageConverter()
	{
		return timeMessageConverter;
	}

	public DateTimeEventEntityConverter getDateTimeEventEntityConverter()
	{
		return dateTimeEventEntityConverter;
	}

	public DetailedTestEventConverter getDetailedTestEventConverter()
	{
		return detailedTestEventConverter;
	}

	public DateEventEntityConverter getDateEventEntityConverter()
	{
		return dateEventEntityConverter;
	}
}

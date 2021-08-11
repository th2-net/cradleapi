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

import com.datastax.oss.driver.api.mapper.annotations.DaoFactory;
import com.datastax.oss.driver.api.mapper.annotations.DaoKeyspace;
import com.datastax.oss.driver.api.mapper.annotations.DaoTable;
import com.datastax.oss.driver.api.mapper.annotations.Mapper;
import com.exactpro.cradle.cassandra.dao.intervals.IntervalOperator;
import com.exactpro.cradle.cassandra.dao.messages.MessageBatchOperator;
import com.exactpro.cradle.cassandra.dao.messages.MessageTestEventConverter;
import com.exactpro.cradle.cassandra.dao.messages.MessageTestEventOperator;
import com.exactpro.cradle.cassandra.dao.messages.TimeMessageOperator;
import com.exactpro.cradle.cassandra.dao.messages.converters.DetailedMessageBatchConverter;
import com.exactpro.cradle.cassandra.dao.testevents.RootTestEventOperator;
import com.exactpro.cradle.cassandra.dao.testevents.TestEventChildrenDatesOperator;
import com.exactpro.cradle.cassandra.dao.testevents.TestEventChildrenOperator;
import com.exactpro.cradle.cassandra.dao.testevents.TestEventMessagesOperator;
import com.exactpro.cradle.cassandra.dao.testevents.TestEventOperator;
import com.exactpro.cradle.cassandra.dao.testevents.TimeTestEventOperator;
import com.exactpro.cradle.cassandra.dao.testevents.converters.RootTestEventConverter;
import com.exactpro.cradle.cassandra.dao.testevents.converters.TestEventChildConverter;
import com.exactpro.cradle.cassandra.dao.testevents.converters.TestEventConverter;
import com.exactpro.cradle.cassandra.dao.testevents.converters.TestEventMessagesConverter;
import com.exactpro.cradle.cassandra.dao.testevents.converters.TimeTestEventConverter;

@Mapper
public interface CassandraDataMapper
{
	@DaoFactory
	MessageBatchOperator messageBatchOperator(@DaoKeyspace String keyspace, @DaoTable String messagesTable);
	
	@DaoFactory
	DetailedMessageBatchConverter detailedMessageBatchConverter();
	
	@DaoFactory
	TimeMessageOperator timeMessageOperator(@DaoKeyspace String keyspace, @DaoTable String timeMessagesTable);
	
	@DaoFactory
	TestEventOperator testEventOperator(@DaoKeyspace String keyspace, @DaoTable String testEventsTable);
	
	@DaoFactory
	TimeTestEventOperator timeTestEventOperator(@DaoKeyspace String keyspace, @DaoTable String timeTestEventsTable);
	
	@DaoFactory
	RootTestEventOperator rootTestEventOperator(@DaoKeyspace String keyspace, @DaoTable String rootTestEventsTable);
	
	@DaoFactory
	TestEventChildrenOperator testEventChildrenOperator(@DaoKeyspace String keyspace, @DaoTable String testEventsChildrenTable);
	
	@DaoFactory
	TestEventChildrenDatesOperator testEventChildrenDatesOperator(@DaoKeyspace String keyspace, @DaoTable String testEventsChildrenDatesTable);
	
	@DaoFactory
	TestEventMessagesOperator testEventMessagesOperator(@DaoKeyspace String keyspace, @DaoTable String testEventsChildrenDatesTable);
	
	@DaoFactory
	MessageTestEventOperator messageTestEventOperator(@DaoKeyspace String keyspace, @DaoTable String testEventsChildrenDatesTable);
	
	@DaoFactory
	TestEventConverter testEventConverter();
	@DaoFactory
	RootTestEventConverter rootTestEventConverter();
	@DaoFactory
	TestEventChildConverter testEventChildConverter();
	@DaoFactory
	TimeTestEventConverter timeTestEventConverter();
	@DaoFactory
	TestEventMessagesConverter testEventMessagesConverter();
	@DaoFactory
	MessageTestEventConverter messageTestEventConverter();

	@DaoFactory
	IntervalOperator intervalOperator(@DaoKeyspace String keyspace, @DaoTable String intervalsTable);
}

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

package com.exactpro.cradle.daomodule.dao;

import com.datastax.oss.driver.api.mapper.annotations.DaoFactory;
import com.datastax.oss.driver.api.mapper.annotations.DaoKeyspace;
import com.datastax.oss.driver.api.mapper.annotations.DaoTable;
import com.datastax.oss.driver.api.mapper.annotations.Mapper;
import com.exactpro.cradle.cassandra.dao.testevents.*;
import com.exactpro.cradle.daomodule.dao.messages.MessageBatchConverter;
import com.exactpro.cradle.daomodule.dao.messages.MessageBatchOperator;
import com.exactpro.cradle.daomodule.dao.messages.MessageTestEventOperator;
import com.exactpro.cradle.daomodule.dao.messages.TimeMessageOperator;
import com.exactpro.cradle.daomodule.dao.testevents.*;

@Mapper
public interface CassandraDataMapper
{
	@DaoFactory
	MessageBatchOperator messageBatchOperator(@DaoKeyspace String keyspace, @DaoTable String messagesTable);
	
	@DaoFactory
	MessageBatchConverter messageBatchConverter();
	
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
}

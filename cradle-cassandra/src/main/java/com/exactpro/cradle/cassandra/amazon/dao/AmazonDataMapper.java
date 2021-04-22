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

package com.exactpro.cradle.cassandra.amazon.dao;

import com.datastax.oss.driver.api.mapper.annotations.DaoFactory;
import com.datastax.oss.driver.api.mapper.annotations.DaoKeyspace;
import com.datastax.oss.driver.api.mapper.annotations.DaoTable;
import com.datastax.oss.driver.api.mapper.annotations.Mapper;
import com.exactpro.cradle.cassandra.amazon.dao.messages.StreamsOperator;
import com.exactpro.cradle.cassandra.amazon.dao.testevents.AmazonTestEventMessagesOperator;
import com.exactpro.cradle.cassandra.amazon.dao.testevents.RootTestEventDatesOperator;

@Mapper
public interface AmazonDataMapper
{
	@DaoFactory
	StreamsOperator streamsOperator(@DaoKeyspace String keyspace, @DaoTable String streamsTable);

	@DaoFactory
	RootTestEventDatesOperator rootTestEventDatesOperator(@DaoKeyspace String keyspace, @DaoTable String rootTestEventsDatesTable);
	
	@DaoFactory
	AmazonTestEventMessagesOperator amazonTestEventMessagesOperator(@DaoKeyspace String keyspace, @DaoTable String testEventsChildrenDatesTable);
}

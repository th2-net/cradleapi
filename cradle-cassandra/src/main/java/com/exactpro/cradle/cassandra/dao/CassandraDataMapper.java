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
import com.exactpro.cradle.cassandra.dao.books.CradleBookOperator;
import com.exactpro.cradle.cassandra.dao.books.PageNameOperator;
import com.exactpro.cradle.cassandra.dao.books.PageOperator;
import com.exactpro.cradle.cassandra.dao.intervals.IntervalOperator;
import com.exactpro.cradle.cassandra.dao.testevents.ScopeOperator;
import com.exactpro.cradle.cassandra.dao.testevents.PageScopesOperator;
import com.exactpro.cradle.cassandra.dao.testevents.TestEventOperator;

@Mapper
public interface CassandraDataMapper
{
	//*** Operators for cradle_info keyspace ***
	
	@DaoFactory
	CradleBookOperator cradleBookOperator(@DaoKeyspace String keyspace, @DaoTable String booksTable);
	
	
	//*** Operators for book's keyspace ***
	
	@DaoFactory
	PageOperator pageOperator(@DaoKeyspace String keyspace, @DaoTable String pagesTable);
	
	@DaoFactory
	PageNameOperator pageNameOperator(@DaoKeyspace String keyspace, @DaoTable String pagesNamesTable);
	
	@DaoFactory
	ScopeOperator scopeOperator(@DaoKeyspace String keyspace, @DaoTable String scopesTable);
	
	
	@DaoFactory
	TestEventOperator testEventOperator(@DaoKeyspace String keyspace, @DaoTable String testEventsTable);
	
	@DaoFactory
	PageScopesOperator pageScopesOperator(@DaoKeyspace String keyspace, @DaoTable String pageScopesTable);
	
	@DaoFactory
	IntervalOperator intervalOperator(@DaoKeyspace String keyspace, @DaoTable String intervalsTable);
}

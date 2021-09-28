/*
 * Copyright 2021-2021 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.cradle.cassandra.dao.testevents;

import java.time.LocalDate;
import java.time.LocalTime;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import com.datastax.oss.driver.api.core.MappedAsyncPagingIterable;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.annotations.Insert;
import com.datastax.oss.driver.api.mapper.annotations.Query;
import com.datastax.oss.driver.api.mapper.annotations.QueryProvider;
import com.exactpro.cradle.cassandra.dao.CommonQueryProvider;

import static com.exactpro.cradle.cassandra.StorageConstants.*;

@Dao
public interface TestEventOperator
{
	@Query("SELECT * FROM ${qualifiedTableId} WHERE "+PAGE+"=:page AND "+START_DATE+"=:startDate "
			+ "AND "+SCOPE+"=:scope AND "+PART+"=:part "
			+ "AND "+START_TIME+"=:startTime AND "+ID+"=:id")
	CompletableFuture<MappedAsyncPagingIterable<TestEventEntity>> get(String page, LocalDate startDate, 
			String scope, String part,
			LocalTime startTime, String id, 
			Function<BoundStatementBuilder, BoundStatementBuilder> attributes);
	
	@QueryProvider(providerClass = CommonQueryProvider.class, entityHelpers = TestEventEntity.class)
	CompletableFuture<MappedAsyncPagingIterable<TestEventEntity>> getByFilter(CassandraTestEventFilter filter,
			Function<BoundStatementBuilder, BoundStatementBuilder> attributes);
	
	@Insert
	CompletableFuture<TestEventEntity> write(TestEventEntity testEvent, Function<BoundStatementBuilder, BoundStatementBuilder> attributes);
	
	@Query("UPDATE ${qualifiedTableId} SET "+SUCCESS+"=:success WHERE "+PAGE+"=:page " 
			+ "AND "+SCOPE+"=:scope AND "+PART+"=:part "
			+ "AND "+START_DATE+"=:startDate "
			+ "AND "+START_TIME+"=:startTime AND "+ID+"=:id AND "+CHUNK+"=0")
	CompletableFuture<Void> updateStatus(String page, String scope, String part,
			LocalDate startDate, LocalTime startTime, String id, 
			boolean success,
			Function<BoundStatementBuilder, BoundStatementBuilder> attributes);
}

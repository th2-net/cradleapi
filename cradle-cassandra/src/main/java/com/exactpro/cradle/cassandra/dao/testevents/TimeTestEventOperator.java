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

package com.exactpro.cradle.cassandra.dao.testevents;

import static com.exactpro.cradle.cassandra.StorageConstants.ID;
import static com.exactpro.cradle.cassandra.StorageConstants.INSTANCE_ID;
import static com.exactpro.cradle.cassandra.StorageConstants.START_DATE;
import static com.exactpro.cradle.cassandra.StorageConstants.START_TIME;
import static com.exactpro.cradle.cassandra.StorageConstants.SUCCESS;

import java.time.LocalDate;
import java.time.LocalTime;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import com.datastax.oss.driver.api.core.PagingIterable;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.annotations.Insert;
import com.datastax.oss.driver.api.mapper.annotations.Query;

@Dao
public interface TimeTestEventOperator
{
	@Query("SELECT * FROM ${qualifiedTableId} WHERE "+INSTANCE_ID+"=:instanceId AND "+
			START_DATE+"=:startDate AND "+START_TIME+">=:timeFrom AND "+START_TIME+"<=:timeTo")
	PagingIterable<TimeTestEventEntity> getTestEvents(UUID instanceId, LocalDate startDate, LocalTime timeFrom, LocalTime timeTo, 
			Function<BoundStatementBuilder, BoundStatementBuilder> attributes);
	
	@Insert
	TimeTestEventEntity writeTestEvent(TimeTestEventEntity timeTestEvent, Function<BoundStatementBuilder, BoundStatementBuilder> attributes);
	
	@Insert
	CompletableFuture<TimeTestEventEntity> writeTestEventAsync(TimeTestEventEntity timeTestEvent, Function<BoundStatementBuilder, BoundStatementBuilder> attributes);
	
	@Query("UPDATE ${qualifiedTableId} SET "+SUCCESS+"=:success WHERE "+INSTANCE_ID+"=:instanceId AND "+
			START_DATE+"=:startDate AND "+START_TIME+"=:startTime AND "+ID+"=:eventId")
	ResultSet updateStatus(UUID instanceId, LocalDate startDate, LocalTime startTime, String eventId, boolean success, 
			Function<BoundStatementBuilder, BoundStatementBuilder> attributes);
	
	@Query("UPDATE ${qualifiedTableId} SET "+SUCCESS+"=:success WHERE "+INSTANCE_ID+"=:instanceId AND "+
			START_DATE+"=:startDate AND "+START_TIME+"=:startTime AND "+ID+"=:eventId")
	CompletableFuture<AsyncResultSet> updateStatusAsync(UUID instanceId, LocalDate startDate, LocalTime startTime, String eventId, boolean success, 
			Function<BoundStatementBuilder, BoundStatementBuilder> attributes);;
}
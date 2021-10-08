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

package com.exactpro.cradle.cassandra.dao.testevents;

import com.datastax.oss.driver.api.core.MappedAsyncPagingIterable;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.annotations.Insert;
import com.datastax.oss.driver.api.mapper.annotations.Query;

import java.time.LocalDate;
import java.time.LocalTime;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static com.exactpro.cradle.cassandra.StorageConstants.*;

@Dao
public interface TimeTestEventOperator
{
	String METADATA_SELECT_START =
			"SELECT " + INSTANCE_ID + ',' + START_DATE + ',' + START_TIME + ',' + ID + ',' + NAME + ',' + TYPE +
					',' + EVENT_BATCH + ',' + END_DATE + ',' + END_TIME + ',' + SUCCESS + ',' + EVENT_COUNT +
					',' + EVENT_BATCH_METADATA + ',' + ROOT + ',' + PARENT_ID;

	@Query("SELECT * FROM ${qualifiedTableId} WHERE " + INSTANCE_ID + "=:instanceId AND " +
			START_DATE + "=:startDate AND " + START_TIME + ">=:timeFrom AND " + START_TIME + "<=:timeTo")
	CompletableFuture<MappedAsyncPagingIterable<TestEventEntity>> getTestEventsDirect(UUID instanceId,
			LocalDate startDate, LocalTime timeFrom, LocalTime timeTo,
			Function<BoundStatementBuilder, BoundStatementBuilder> attributes);

	@Query(METADATA_SELECT_START +
			" FROM ${qualifiedTableId} WHERE " + INSTANCE_ID + "=:instanceId AND " +
			START_DATE + "=:startDate AND " + START_TIME + ">=:timeFrom AND " + START_TIME + "<=:timeTo")
	CompletableFuture<MappedAsyncPagingIterable<TestEventMetadataEntity>> getTestEventsMetadataDirect(UUID instanceId,
			LocalDate startDate, LocalTime timeFrom, LocalTime timeTo,
			Function<BoundStatementBuilder, BoundStatementBuilder> attributes);

	@Query("SELECT * FROM ${qualifiedTableId} WHERE " + INSTANCE_ID + "=:instanceId AND " + PARENT_ID + "=:parentId AND " +
			START_DATE + "=:startDate AND " + START_TIME + ">=:timeFrom AND " + START_TIME + "<=:timeTo")
	CompletableFuture<MappedAsyncPagingIterable<TestEventEntity>> getTestEventsDirect(UUID instanceId, String parentId,
			LocalDate startDate, LocalTime timeFrom, LocalTime timeTo,
			Function<BoundStatementBuilder, BoundStatementBuilder> attributes);

	@Query(METADATA_SELECT_START +
			" FROM ${qualifiedTableId} WHERE " + INSTANCE_ID + "=:instanceId AND " + PARENT_ID + "=:parentId AND " +
			START_DATE + "=:startDate AND " + START_TIME + ">=:timeFrom AND " + START_TIME + "<=:timeTo")
	CompletableFuture<MappedAsyncPagingIterable<TestEventMetadataEntity>> getTestEventsMetadataDirect(UUID instanceId,
			String parentId, LocalDate startDate, LocalTime timeFrom, LocalTime timeTo,
			Function<BoundStatementBuilder, BoundStatementBuilder> attributes);

	@Query("SELECT * FROM ${qualifiedTableId} WHERE " + INSTANCE_ID + "=:instanceId AND " +
			START_DATE + "=:startDate AND " + START_TIME + ">=:timeFrom AND " + START_TIME + "<=:timeTo ORDER BY " +
			START_TIME + " DESC, " + ID + " DESC")
	CompletableFuture<MappedAsyncPagingIterable<TestEventEntity>> getTestEventsReverse(UUID instanceId,
			LocalDate startDate, LocalTime timeFrom, LocalTime timeTo,
			Function<BoundStatementBuilder, BoundStatementBuilder> attributes);

	@Query(METADATA_SELECT_START +
			" FROM ${qualifiedTableId} WHERE " + INSTANCE_ID + "=:instanceId AND " +
			START_DATE + "=:startDate AND " + START_TIME + ">=:timeFrom AND " + START_TIME + "<=:timeTo ORDER BY " +
			START_TIME + " DESC, " + ID + " DESC")
	CompletableFuture<MappedAsyncPagingIterable<TestEventMetadataEntity>> getTestEventsMetadataReverse(UUID instanceId,
			LocalDate startDate, LocalTime timeFrom, LocalTime timeTo,
			Function<BoundStatementBuilder, BoundStatementBuilder> attributes);

// TODO When using filtration by index and ORDER BY clause in query, cassandra throws exception 
//  'ORDER BY with 2ndary indexes is not supported.'
//	@Query("SELECT * FROM ${qualifiedTableId} WHERE " + INSTANCE_ID + "=:instanceId AND " + START_DATE + "=:startDate AND "
//			+ PARENT_ID + "=:parentId AND "+ START_TIME + ">=:timeFrom AND " + START_TIME + "<=:timeTo ORDER BY " +
//			START_TIME + " DESC, " + ID + " DESC")
//	CompletableFuture<MappedAsyncPagingIterable<TestEventEntity>> getTestEventsReverse(UUID instanceId, String parentId,
//			LocalDate startDate, LocalTime timeFrom, LocalTime timeTo,
//			Function<BoundStatementBuilder, BoundStatementBuilder> attributes);

	@Query("SELECT * FROM ${qualifiedTableId} WHERE " + INSTANCE_ID + "=:instanceId AND " +
			START_DATE + "=:startDate AND " + START_TIME + "=:startTime AND " + ID + "=:eventId")
	CompletableFuture<DetailedTestEventEntity> get(UUID instanceId, LocalDate startDate, LocalTime startTime,
			String eventId, Function<BoundStatementBuilder, BoundStatementBuilder> attributes);

	@Query("SELECT " + INSTANCE_ID + ", " + START_DATE + " FROM ${qualifiedTableId} WHERE " + PARENT_ID + "=:parentId")
	ResultSet getTestEventsDates(String parentId, Function<BoundStatementBuilder, BoundStatementBuilder> attributes);

	@Insert
	CompletableFuture<Void> writeTestEvent(DetailedTestEventEntity timeTestEvent,
			Function<BoundStatementBuilder, BoundStatementBuilder> attributes);
	
	@Query("UPDATE ${qualifiedTableId} SET "+SUCCESS+"=:success WHERE "+INSTANCE_ID+"=:instanceId AND "+
			START_DATE+"=:startDate AND "+START_TIME+"=:startTime AND "+ID+"=:eventId")
	CompletableFuture<Void> updateStatus(UUID instanceId, LocalDate startDate, LocalTime startTime,
					String eventId, boolean success, Function<BoundStatementBuilder, BoundStatementBuilder> attributes);
}

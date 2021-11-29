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

package com.exactpro.cradle.cassandra.dao.messages;

import com.datastax.oss.driver.api.core.MappedAsyncPagingIterable;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.mapper.annotations.*;
import com.exactpro.cradle.cassandra.dao.CommonQueryProvider;

import java.time.LocalDate;
import java.time.LocalTime;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

import static com.exactpro.cradle.cassandra.StorageConstants.*;

@Dao
public interface MessageBatchOperator
{
	@Select
	CompletableFuture<MessageBatchEntity> get(String page, String sessionAlias,
			String direction, LocalDate messageDate, LocalTime messageTime, long sequence,
			Function<BoundStatementBuilder, BoundStatementBuilder> attributes);

	@Query("SELECT " + SEQUENCE + ", " + MESSAGE_TIME + " FROM ${qualifiedTableId} WHERE " + PAGE + "=:page"
			+ " AND " + SESSION_ALIAS + "=:sessionAlias AND " + DIRECTION + "=:direction AND "
			+ MESSAGE_DATE + "=:messageDate AND (" + MESSAGE_TIME + ", " + SEQUENCE + ")<=(:messageTime, :sequence)"
			+ " ORDER BY " + MESSAGE_DATE + " DESC, " + MESSAGE_TIME + " DESC, " + SEQUENCE + " DESC LIMIT 1")
	CompletableFuture<Row> getNearestTimeAndSequenceBefore(String page, String sessionAlias,
			String direction, LocalDate messageDate, LocalTime messageTime, long sequence,
			Function<BoundStatementBuilder, BoundStatementBuilder> attributes);

	@Query("SELECT " + MESSAGE_TIME + " FROM ${qualifiedTableId} WHERE " + PAGE + "=:page"
			+ " AND " + SESSION_ALIAS + "=:sessionAlias AND " + DIRECTION + "=:direction AND "
			+ MESSAGE_DATE + "=:messageDate AND " + MESSAGE_TIME + "<=:messageTime"
			+ " ORDER BY " + MESSAGE_DATE + " DESC, " + MESSAGE_TIME + " DESC, " + SEQUENCE + " DESC LIMIT 1")
	CompletableFuture<Row> getNearestTime(String page, String sessionAlias,
			String direction, LocalDate messageDate, LocalTime messageTime,
			Function<BoundStatementBuilder, BoundStatementBuilder> attributes);

	@Query("SELECT " + LAST_SEQUENCE + " FROM ${qualifiedTableId} WHERE " + PAGE + "=:page"
			+ " AND " + SESSION_ALIAS + "=:sessionAlias AND " + DIRECTION + "=:direction "
			+ " ORDER BY " + MESSAGE_DATE + " DESC, " + MESSAGE_TIME + " DESC, " + SEQUENCE + " DESC LIMIT 1" )
	CompletableFuture<Row> getLastSequence(String page, String sessionAlias, String direction,
			Function<BoundStatementBuilder, BoundStatementBuilder> attributes);

	@Query("SELECT " + SEQUENCE + " FROM ${qualifiedTableId} WHERE " + PAGE + "=:page"
			+ " AND " + SESSION_ALIAS + "=:sessionAlias AND " + DIRECTION + "=:direction "
			+ " ORDER BY " + MESSAGE_DATE + " ASC, " + MESSAGE_TIME + " ASC, " + SEQUENCE + " ASC LIMIT 1" )
	CompletableFuture<Row> getFirstSequence(String page, String sessionAlias, String direction,
			Function<BoundStatementBuilder, BoundStatementBuilder> attributes);

	@QueryProvider(providerClass = CommonQueryProvider.class, entityHelpers = MessageBatchEntity.class)
	CompletableFuture<MappedAsyncPagingIterable<MessageBatchEntity>> getByFilter(CassandraStoredMessageFilter filter, 
			ExecutorService composingService,
			Function<BoundStatementBuilder, BoundStatementBuilder> attributes);

	@Insert
	CompletableFuture<MessageBatchEntity> write(MessageBatchEntity batch,
			Function<BoundStatementBuilder, BoundStatementBuilder> attributes);
	
	@Delete(entityClass = MessageBatchEntity.class)
	void remove(String page, String sessionAlias, String direction, Function<BoundStatementBuilder, BoundStatementBuilder> attributes);
}

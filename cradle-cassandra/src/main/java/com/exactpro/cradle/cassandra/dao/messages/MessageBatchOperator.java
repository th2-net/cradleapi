/*
 * Copyright 2021-2024 Exactpro (Exactpro Systems Limited)
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
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.annotations.QueryProvider;
import com.datastax.oss.driver.api.mapper.annotations.Select;
import com.datastax.oss.driver.api.mapper.annotations.Query;
import com.datastax.oss.driver.api.mapper.annotations.Delete;

import com.exactpro.cradle.cassandra.dao.BoundStatementBuilderWrapper;
import com.exactpro.cradle.cassandra.dao.CommonQueryProvider;
import com.exactpro.cradle.cassandra.retries.SelectQueryExecutor;

import java.time.LocalDate;
import java.time.LocalTime;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static com.exactpro.cradle.cassandra.dao.messages.MessageBatchEntity.*;

@Dao
public interface MessageBatchOperator {
	@Select
	CompletableFuture<MessageBatchEntity> get(String book, String page, String sessionAlias,
			String direction, LocalDate messageDate, LocalTime messageTime, long sequence,
			Function<BoundStatementBuilder, BoundStatementBuilder> attributes);

	@Query( "SELECT " +
				FIELD_FIRST_MESSAGE_DATE + ", " +
				FIELD_FIRST_MESSAGE_TIME + ", " +
				FIELD_SEQUENCE + ", " +
				FIELD_LAST_MESSAGE_DATE + ", " +
				FIELD_LAST_MESSAGE_TIME + ", " +
				FIELD_LAST_SEQUENCE + " " +
			"FROM ${qualifiedTableId} " +
			"WHERE " +
				FIELD_BOOK + " =:book AND " +
				FIELD_PAGE + " =:page AND " +
				FIELD_SESSION_ALIAS + " =:sessionAlias AND " +
				FIELD_DIRECTION + " =:direction AND " +
			    "(" + FIELD_FIRST_MESSAGE_DATE + ", " + FIELD_FIRST_MESSAGE_TIME + ", " + FIELD_SEQUENCE + ") <= " +
							"(:messageDate, :messageTime, :sequence) " +
			"ORDER BY " +
				FIELD_FIRST_MESSAGE_DATE + " DESC, " +
				FIELD_FIRST_MESSAGE_TIME + " DESC, " +
				FIELD_SEQUENCE + " DESC LIMIT 1")
	CompletableFuture<Row> getNearestBatchTimeAndSequenceBefore(String book, String page, String sessionAlias,
																String direction, LocalDate messageDate, LocalTime messageTime, long sequence,
																Function<BoundStatementBuilder, BoundStatementBuilder> attributes);



	@Query( "SELECT " +
				FIELD_FIRST_MESSAGE_TIME + " " +
			"FROM ${qualifiedTableId} " +
			"WHERE " +
				FIELD_BOOK + "=:book AND " +
				FIELD_PAGE + "=:page AND " +
				FIELD_SESSION_ALIAS + " =:sessionAlias AND " +
				FIELD_DIRECTION + " =:direction AND " +
				"(" + FIELD_FIRST_MESSAGE_DATE + ", " + FIELD_FIRST_MESSAGE_TIME + ") <= (:messageDate, :messageTime) " +
			"ORDER BY " +
				FIELD_FIRST_MESSAGE_DATE + " DESC, " +
				FIELD_FIRST_MESSAGE_TIME + " DESC, " +
				FIELD_SEQUENCE + " DESC LIMIT 1")
	CompletableFuture<Row> getNearestTime(String book, String page, String sessionAlias,
			String direction, LocalDate messageDate, LocalTime messageTime,
			Function<BoundStatementBuilder, BoundStatementBuilder> attributes);



	@Query( "SELECT " +
				FIELD_LAST_SEQUENCE + " " +
			"FROM ${qualifiedTableId} " +
			"WHERE " +
				FIELD_BOOK + "=:book AND " +
				FIELD_PAGE + " =:page AND " +
				FIELD_SESSION_ALIAS + " =:sessionAlias AND " +
				FIELD_DIRECTION + " =:direction " +
			"ORDER BY " +
				FIELD_FIRST_MESSAGE_DATE + " DESC, " +
				FIELD_FIRST_MESSAGE_TIME + " DESC, " +
				FIELD_SEQUENCE + " DESC LIMIT 1" )
	CompletableFuture<Row> getLastSequence(String book, String page, String sessionAlias, String direction,
			Function<BoundStatementBuilder, BoundStatementBuilder> attributes);

	@Query( "SELECT " +
				FIELD_SEQUENCE + " " +
			"FROM ${qualifiedTableId} " +
			"WHERE " +
				FIELD_BOOK + " =:book AND " +
				FIELD_PAGE + " =:page AND " +
				FIELD_SESSION_ALIAS + " =:sessionAlias AND " +
				FIELD_DIRECTION + " =:direction " +
			"ORDER BY " +
				FIELD_FIRST_MESSAGE_DATE + " ASC, " +
				FIELD_FIRST_MESSAGE_TIME + " ASC, " +
				FIELD_SEQUENCE + " ASC LIMIT 1" )
	CompletableFuture<Row> getFirstSequence(String book, String page, String sessionAlias, String direction,
			Function<BoundStatementBuilder, BoundStatementBuilder> attributes);

	@QueryProvider(providerClass = CommonQueryProvider.class, entityHelpers = MessageBatchEntity.class)
	CompletableFuture<MappedAsyncPagingIterable<MessageBatchEntity>> getByFilter(CassandraStoredMessageFilter filter,
			SelectQueryExecutor selectExecutor, String queryInfo,
			Function<BoundStatementBuilder, BoundStatementBuilder> attributes);

	@QueryProvider(providerClass = MessageBatchInserter.class, entityHelpers = MessageBatchEntity.class, providerMethod = "insert")
	CompletableFuture<AsyncResultSet> write(MessageBatchEntity batch,
											Function<BoundStatementBuilderWrapper, BoundStatementBuilderWrapper> attributes);
	
	@Delete(entityClass = MessageBatchEntity.class)
	void remove(String book, String page, String sessionAlias, String direction,
										Function<BoundStatementBuilder, BoundStatementBuilder> attributes);
}

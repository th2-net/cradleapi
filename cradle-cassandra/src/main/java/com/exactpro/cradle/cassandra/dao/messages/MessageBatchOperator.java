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
import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.annotations.Query;

import java.time.LocalDate;
import java.time.LocalTime;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static com.exactpro.cradle.cassandra.StorageConstants.*;

@Dao
public interface MessageBatchOperator
{
	@Query("SELECT * FROM ${qualifiedTableId} WHERE " + PAGE + "=:page AND " + START_DATE + "=:messageDate "
			+ "AND " + SESSION_ALIAS + "=:sessionAlias AND " + DIRECTION + "=:direction AND " + PART + "=:part"
			+ "AND " + MESSAGE_TIME + "=:messsageTime AND " + SEQUENCE + "=:sequence")
	CompletableFuture<MappedAsyncPagingIterable<MessageBatchEntity>> get(String page, LocalDate messageDate,
			String sessionAlias, String direction, String part, LocalTime messageTime, long sequence,
			Function<BoundStatementBuilder, BoundStatementBuilder> attributes);

//	@Insert
//	CompletableFuture<DetailedMessageBatchEntity> writeMessageBatch(DetailedMessageBatchEntity message, 
//			Function<BoundStatementBuilder, BoundStatementBuilder> attributes);
}

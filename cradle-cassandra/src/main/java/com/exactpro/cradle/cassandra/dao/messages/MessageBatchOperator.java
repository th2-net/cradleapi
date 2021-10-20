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

package com.exactpro.cradle.cassandra.dao.messages;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import com.datastax.oss.driver.api.core.MappedAsyncPagingIterable;
import com.datastax.oss.driver.api.core.PagingIterable;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.annotations.Insert;
import com.datastax.oss.driver.api.mapper.annotations.Query;
import com.datastax.oss.driver.api.mapper.annotations.QueryProvider;
import com.datastax.oss.driver.api.mapper.annotations.Select;
import com.exactpro.cradle.messages.StoredMessageFilter;

import static com.exactpro.cradle.cassandra.StorageConstants.*;

@Dao
public interface MessageBatchOperator
{
	@Select
	PagingIterable<DetailedMessageBatchEntity> get(UUID instanceId, String streamName, 
			Function<BoundStatementBuilder, BoundStatementBuilder> attributes);
	
	@Select
	PagingIterable<DetailedMessageBatchEntity> get(UUID instanceId, String streamName, String direction, 
			Function<BoundStatementBuilder, BoundStatementBuilder> attributes);
	
	@Select
	DetailedMessageBatchEntity get(UUID instanceId, String streamName, String direction, long messageIndex, 
			Function<BoundStatementBuilder, BoundStatementBuilder> attributes);
	
	@Query("SELECT * FROM ${qualifiedTableId} WHERE "
			+INSTANCE_ID+"=:instanceId AND "+STREAM_NAME+"=:streamName AND "+DIRECTION+"=:direction AND "
			+MESSAGE_INDEX+">=:fromIndex AND "+MESSAGE_INDEX+"<=:toIndex")
	CompletableFuture<MappedAsyncPagingIterable<DetailedMessageBatchEntity>> getMessageBatches(UUID instanceId, String streamName, String direction, 
			long fromIndex, long toIndex, Function<BoundStatementBuilder, BoundStatementBuilder> attributes);

	@Query("SELECT * FROM ${qualifiedTableId} WHERE "
			+INSTANCE_ID+"=:instanceId AND "+STREAM_NAME+"=:streamName AND "+DIRECTION+"=:direction AND "
			+MESSAGE_INDEX+"<=:toIndex ORDER BY "+DIRECTION+" DESC, "+MESSAGE_INDEX+" DESC")
	PagingIterable<DetailedMessageBatchEntity> getMessageBatchesReversed(UUID instanceId, String streamName, String direction, long toIndex, 
			Function<BoundStatementBuilder, BoundStatementBuilder> attributes);
	
	@Query("SELECT * FROM ${qualifiedTableId} WHERE "
			+INSTANCE_ID+"=:instanceId AND "+STREAM_NAME+"=:streamName AND "+DIRECTION+"=:direction AND "
			+MESSAGE_INDEX+"<=:messageIndex ORDER BY "+DIRECTION+" DESC, "+MESSAGE_INDEX+" DESC LIMIT 1")
	CompletableFuture<DetailedMessageBatchEntity> getMessageBatch(UUID instanceId, String streamName, String direction, long messageIndex, 
			Function<BoundStatementBuilder, BoundStatementBuilder> attributes);
	
	@Select
	PagingIterable<DetailedMessageBatchEntity> getAll(Function<BoundStatementBuilder, BoundStatementBuilder> attributes);
	
	@Query("SELECT " + MESSAGE_INDEX + " FROM ${qualifiedTableId} WHERE "
			+INSTANCE_ID+"=:instanceId AND "+STREAM_NAME+"=:streamName AND "+DIRECTION+"=:direction LIMIT 1")
	CompletableFuture<Row> getFirstIndex(UUID instanceId, String streamName, String direction,
			Function<BoundStatementBuilder, BoundStatementBuilder> attributes);
	
	@Query("SELECT " +LAST_MESSAGE_INDEX+ " FROM ${qualifiedTableId} WHERE "
			+INSTANCE_ID+"=:instanceId AND "+STREAM_NAME+"=:streamName AND "+DIRECTION+"=:direction "
			+ "ORDER BY "+DIRECTION+" DESC, "+MESSAGE_INDEX+" DESC LIMIT 1")
	CompletableFuture<Row> getLastIndex(UUID instanceId, String streamName, String direction,
			Function<BoundStatementBuilder, BoundStatementBuilder> attributes);
	
	@QueryProvider(providerClass = MessageBatchQueryProvider.class, entityHelpers = DetailedMessageBatchEntity.class)
	CompletableFuture<MappedAsyncPagingIterable<DetailedMessageBatchEntity>> filterMessages(UUID instanceId,
			StoredMessageFilter filter, MessageBatchOperator mbOperator,
			TimeMessageOperator tmOperator, Function<BoundStatementBuilder, BoundStatementBuilder> attributes);
	
	@Query("SELECT DISTINCT "+INSTANCE_ID+", "+STREAM_NAME+" from ${qualifiedTableId}")
	PagingIterable<StreamEntity> getStreams(Function<BoundStatementBuilder, BoundStatementBuilder> attributes);
	
	@Insert
	CompletableFuture<DetailedMessageBatchEntity> writeMessageBatch(DetailedMessageBatchEntity message, 
			Function<BoundStatementBuilder, BoundStatementBuilder> attributes);
}

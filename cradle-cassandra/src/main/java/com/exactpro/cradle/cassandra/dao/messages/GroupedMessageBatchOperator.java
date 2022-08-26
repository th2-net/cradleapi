/*
 * Copyright 2022-2022 Exactpro (Exactpro Systems Limited)
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
import com.datastax.oss.driver.api.mapper.annotations.Insert;
import com.datastax.oss.driver.api.mapper.annotations.Query;

import java.time.LocalDate;
import java.time.LocalTime;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static com.exactpro.cradle.cassandra.StorageConstants.*;

@Dao
public interface GroupedMessageBatchOperator
{
	@Query("SELECT * FROM ${qualifiedTableId} WHERE "
			+INSTANCE_ID+"=:instanceId AND "+ STREAM_GROUP +"=:groupName"+
			" AND (" + MESSAGE_DATE + ", " + MESSAGE_TIME +")>=(:dateFrom, :timeFrom)"+
			" AND (" + MESSAGE_DATE + ", " + MESSAGE_TIME +")<=(:dateTo, :timeTo)")
	CompletableFuture<MappedAsyncPagingIterable<GroupedMessageBatchEntity>> getByTimeRange(UUID instanceId,
			String groupName, LocalDate dateFrom, LocalTime timeFrom, LocalDate dateTo,
			LocalTime timeTo, Function<BoundStatementBuilder, BoundStatementBuilder> attributes);

	@Query("SELECT * FROM ${qualifiedTableId} WHERE "
			+INSTANCE_ID+"=:instanceId AND "+ STREAM_GROUP +"=:groupName"+
			" ORDER BY " + MESSAGE_DATE + " DESC, " + MESSAGE_TIME + " DESC LIMIT 1")
	CompletableFuture<GroupedMessageBatchEntity> getLastMessageBatch (UUID instanceId, String groupName);

	@Insert
	CompletableFuture<Void> writeMessageBatch(GroupedMessageBatchEntity message,
			Function<BoundStatementBuilder, BoundStatementBuilder> attributes);
}

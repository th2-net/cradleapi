/*
 * Copyright 2020-2022 Exactpro (Exactpro Systems Limited)
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

import com.datastax.oss.driver.api.core.MappedAsyncPagingIterable;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.annotations.Query;
import com.datastax.oss.driver.api.mapper.annotations.Update;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static com.exactpro.cradle.cassandra.StorageConstants.*;

@Dao
public interface EntityStatisticsOperator {

    @Query("SELECT * FROM ${qualifiedTableId}  WHERE " +
            ENTITY_TYPE + "=:entityType AND " +
            FRAME_TYPE + "=:frameType AND " +
            FRAME_START + ">=:frameStart AND " +
            FRAME_START + "<:frameEnd")
    CompletableFuture<MappedAsyncPagingIterable<EntityStatisticsEntity>> getStatistics (
            Byte entityType,
            Byte frameType,
            Instant frameStart,
            Instant frameEnd,
            Function<BoundStatementBuilder, BoundStatementBuilder> attributes
    );

    @Update
    @Query("UPDATE ${qualifiedTableId}  SET entity_count = entity_count + :count, entity_size = entity_size + :size WHERE " +
            ENTITY_TYPE + "=:entityType AND " +
            FRAME_TYPE + "=:frameType AND " +
            FRAME_START + "=:frameStart")
    CompletableFuture<Void> update(
            Byte entityType,
            Byte frameType,
            Instant frameStart,
            long count,
            long size,
            Function<BoundStatementBuilder, BoundStatementBuilder> attributes
    );

}
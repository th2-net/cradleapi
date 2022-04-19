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
import com.datastax.oss.driver.api.mapper.annotations.*;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static com.exactpro.cradle.cassandra.StorageConstants.*;

@Dao
public interface MessageStatisticsOperator {

    @Query("SELECT * FROM ${qualifiedTableId}  WHERE " + PAGE + "=:page AND " +
            SESSION_ALIAS + "=:sessionAlias AND " +
            DIRECTION + "=:direction AND " +
            FRAME_TYPE + "=:frameType AND " +
            FRAME_START + ">=:frameStart AND " +
            FRAME_START + "<:frameEnd")
    CompletableFuture<MappedAsyncPagingIterable<MessageStatisticsEntity>> getStatistics (
            String page,
            String sessionAlias,
            String direction,
            Byte frameType,
            Instant frameStart,
            Instant frameEnd,
            Function<BoundStatementBuilder, BoundStatementBuilder> attributes
    );



    @Increment(entityClass = MessageStatisticsEntity.class)
    CompletableFuture<Void> update(
            String page,
            String sessionAlias,
            String direction,
            Byte frameType,
            Instant frameStart,
            @CqlName(ENTITY_COUNT) long count,
            @CqlName(ENTITY_SIZE) long size,
            Function<BoundStatementBuilder, BoundStatementBuilder> attributes
    );

}
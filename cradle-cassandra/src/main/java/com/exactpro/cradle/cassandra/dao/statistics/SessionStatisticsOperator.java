/*
 * Copyright 2020-2023 Exactpro (Exactpro Systems Limited)
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
package com.exactpro.cradle.cassandra.dao.statistics;

import com.datastax.oss.driver.api.core.MappedAsyncPagingIterable;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BatchStatementBuilder;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.annotations.Delete;
import com.datastax.oss.driver.api.mapper.annotations.Query;
import com.datastax.oss.driver.api.mapper.annotations.QueryProvider;

import java.time.Instant;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static com.exactpro.cradle.cassandra.dao.statistics.SessionStatisticsEntity.*;

@Dao
public interface SessionStatisticsOperator {

    @QueryProvider(providerClass = SessionStatisticsBatchInserter.class, entityHelpers = SessionStatisticsEntity.class, providerMethod = "insert")
    CompletableFuture<AsyncResultSet> write(Collection<SessionStatisticsEntity> sessionBatch,
                                            Function<BatchStatementBuilder, BatchStatementBuilder> attributes);

    @Query( "SELECT * FROM ${qualifiedTableId} " +
            "WHERE " +
                FIELD_BOOK + "=:book AND " +
                FIELD_PAGE + "=:page AND " +
                FIELD_RECORD_TYPE + "=:recordType AND " +
                FIELD_FRAME_TYPE + "=:frameType AND " +
                FIELD_FRAME_START + ">=:frameStart AND " +
                FIELD_FRAME_START + "<:frameEnd")
    CompletableFuture<MappedAsyncPagingIterable<SessionStatisticsEntity>> getStatistics (
                String book,
                String page,
                Byte recordType,
                Byte frameType,
                Instant frameStart,
                Instant frameEnd,
                Function<BoundStatementBuilder, BoundStatementBuilder> attributes);

    @Delete(entityClass = SessionStatisticsEntity.class)
    void remove(String book, String page, Byte recordType, Byte frameType,
                Function<BoundStatementBuilder, BoundStatementBuilder> attributes);
}
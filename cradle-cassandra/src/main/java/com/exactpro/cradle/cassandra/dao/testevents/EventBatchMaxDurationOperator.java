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
package com.exactpro.cradle.cassandra.dao.testevents;

import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.annotations.Query;
import com.datastax.oss.driver.api.mapper.annotations.Select;

import java.time.LocalDate;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static com.exactpro.cradle.cassandra.StorageConstants.*;

@Dao
public interface EventBatchMaxDurationOperator {


    @Query("INSERT into ${qualifiedTableId} (" + INSTANCE_ID + ", " + START_DATE + ", " + MAX_BATCH_DURATION + ") "
            + "VALUES (:uuid, :startDate, :duration) "
            + "IF NOT EXISTS")
    CompletableFuture<Void> writeMaxDuration(UUID uuid, LocalDate startDate, long duration, Function<BoundStatementBuilder, BoundStatementBuilder> attributes);


    @Query("UPDATE ${qualifiedTableId} SET " + MAX_BATCH_DURATION + "= :duration "
            + "WHERE " + INSTANCE_ID + "=:uuid AND " + START_DATE + "= :startDate "
            + "IF " + MAX_BATCH_DURATION + "<:maxDuration")
    void updateMaxDuration(UUID uuid, LocalDate startDate, Long duration, Long maxDuration, Function<BoundStatementBuilder, BoundStatementBuilder> attributes);

    @Select
   EventBatchMaxDurationEntity getMaxDuration(UUID uuid, LocalDate localDate, Function<BoundStatementBuilder, BoundStatementBuilder> attributes);
}

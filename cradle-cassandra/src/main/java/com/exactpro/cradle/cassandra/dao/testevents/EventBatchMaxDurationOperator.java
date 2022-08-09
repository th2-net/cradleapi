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
import com.datastax.oss.driver.api.mapper.annotations.*;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static com.exactpro.cradle.cassandra.dao.testevents.EventBatchMaxDurationEntity.*;

@Dao
public interface EventBatchMaxDurationOperator {


    @Insert(ifNotExists = true)
    CompletableFuture<Void> writeMaxDuration (EventBatchMaxDurationEntity entity,
                                              Function<BoundStatementBuilder, BoundStatementBuilder> attributes);


    @Query( "UPDATE ${qualifiedTableId} " +
            "SET "   + FIELD_MAX_BATCH_DURATION + " = :duration " +
            "WHERE " +
                FIELD_BOOK + " =:book AND " +
                FIELD_PAGE + " =:page AND " +
                FIELD_SCOPE + " = :scope " +
            "IF " + FIELD_MAX_BATCH_DURATION + " <:maxDuration")
    void updateMaxDuration(String book, String page, String scope, Long duration, Long maxDuration,
                           Function<BoundStatementBuilder, BoundStatementBuilder> attributes);


    @Delete(entityClass = EventBatchMaxDurationEntity.class)
    CompletableFuture<Void> removeMaxDurations (String book, String page, String scope);


    @Select
    EventBatchMaxDurationEntity getMaxDuration(String book, String page, String scope,
                                               Function<BoundStatementBuilder, BoundStatementBuilder> attributes);
}
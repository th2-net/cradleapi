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

package com.exactpro.cradle.cassandra.dao.intervals;

import com.datastax.oss.driver.api.core.MappedAsyncPagingIterable;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.annotations.Insert;
import com.datastax.oss.driver.api.mapper.annotations.Query;

import java.time.LocalDate;
import java.time.LocalTime;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static com.exactpro.cradle.cassandra.dao.intervals.IntervalEntity.*;
@Dao
public interface IntervalOperator {
    @Insert(ifNotExists = true)
    CompletableFuture<Boolean> writeInterval(IntervalEntity IntervalEntity, Function<BoundStatementBuilder,
                                                            BoundStatementBuilder> attributes);

    @Query( "SELECT * FROM ${qualifiedTableId} " +
            "WHERE " +
                FIELD_BOOK + " =:book AND " +
                FIELD_INTERVAL_START_DATE +" =:intervalStartDate AND " +
                FIELD_CRAWLER_NAME + " =:crawlerName AND " +
                FIELD_CRAWLER_VERSION + " =:crawlerVersion AND " +
                FIELD_CRAWLER_TYPE + " =:crawlerType AND " +
                FIELD_INTERVAL_START_TIME + " >=:intervalStartTime AND " +
                FIELD_INTERVAL_START_TIME +"<=:intervalEndTime")
    CompletableFuture<MappedAsyncPagingIterable<IntervalEntity>> getIntervals(String book,
                        LocalDate intervalStartDate, LocalTime intervalStartTime, LocalTime intervalEndTime,
                        String crawlerName, String crawlerVersion, String crawlerType,
                        Function<BoundStatementBuilder, BoundStatementBuilder> attributes);

    @Query( "UPDATE ${qualifiedTableId} " +
            "SET " +
                FIELD_INTERVAL_LAST_UPDATE_TIME + " =:lastUpdateTime, " +
                FIELD_INTERVAL_LAST_UPDATE_DATE + " =:lastUpdateDate " +

            "WHERE " +
                FIELD_BOOK + " =:book AND " +
                FIELD_INTERVAL_START_DATE +" =:intervalStartDate AND " +
                FIELD_CRAWLER_NAME + " =:crawlerName AND " +
                FIELD_CRAWLER_VERSION +" =:crawlerVersion AND " +
                FIELD_CRAWLER_TYPE + " =:crawlerType AND " +
                FIELD_INTERVAL_START_TIME + " =:intervalStartTime" +
            " IF " +
                FIELD_INTERVAL_LAST_UPDATE_TIME + " =:previousLastUpdateTime AND " +
                FIELD_INTERVAL_LAST_UPDATE_DATE + " =:previousLastUpdateDate")
    CompletableFuture<AsyncResultSet> setIntervalLastUpdateTimeAndDate(String book,
                                        LocalDate intervalStartDate, LocalTime intervalStartTime,
                                        LocalTime lastUpdateTime, LocalDate lastUpdateDate, LocalTime previousLastUpdateTime,
                                        LocalDate previousLastUpdateDate, String crawlerName, String crawlerVersion,
                                        String crawlerType,
                                        Function<BoundStatementBuilder, BoundStatementBuilder> attributes);


    @Query( "UPDATE ${qualifiedTableId} " +
            "SET " +
                FIELD_RECOVERY_STATE_JSON + " =:recoveryState, " +
                FIELD_INTERVAL_LAST_UPDATE_TIME + " =:lastUpdateTime, " +
                FIELD_INTERVAL_LAST_UPDATE_DATE + " =:lastUpdateDate " +

            "WHERE " +
                FIELD_BOOK + " =:book AND " +
                FIELD_INTERVAL_START_DATE + " =:intervalDate AND " +
                FIELD_CRAWLER_NAME + " =:crawlerName AND " +
                FIELD_CRAWLER_VERSION + " =:crawlerVersion AND " +
                FIELD_CRAWLER_TYPE + " =:crawlerType AND " +
                FIELD_INTERVAL_START_TIME + " =:intervalStartTime" +
            " IF " +
                FIELD_RECOVERY_STATE_JSON + " =:previousRecoveryState AND " +
                FIELD_INTERVAL_LAST_UPDATE_TIME + " =:previousLastUpdateTime AND " +
                FIELD_INTERVAL_LAST_UPDATE_DATE + " =:previousLastUpdateDate")
    CompletableFuture<AsyncResultSet> updateRecoveryState(String book, LocalDate intervalDate, LocalTime intervalStartTime, LocalTime lastUpdateTime, LocalDate lastUpdateDate, String recoveryState, String previousRecoveryState, LocalTime previousLastUpdateTime, LocalDate previousLastUpdateDate, String crawlerName, String crawlerVersion, String crawlerType, Function<BoundStatementBuilder, BoundStatementBuilder> attributes);



    @Query( "UPDATE ${qualifiedTableId} " +
            "SET " +
                FIELD_INTERVAL_PROCESSED + " =:processed, " +
                FIELD_INTERVAL_LAST_UPDATE_TIME + " =:lastUpdateTime, " +
                FIELD_INTERVAL_LAST_UPDATE_DATE + " =:lastUpdateDate " +

            "WHERE " +
                FIELD_BOOK + " =:book AND " +
                FIELD_INTERVAL_START_DATE + " =:intervalDate AND " +
                FIELD_CRAWLER_NAME + " =:crawlerName AND " +
                FIELD_CRAWLER_VERSION + " =:crawlerVersion AND " +
                FIELD_CRAWLER_TYPE + " =:crawlerType AND " +
                FIELD_INTERVAL_START_TIME + " =:intervalStartTime" +
            " IF " +
                FIELD_INTERVAL_PROCESSED + " =:previousProcessed AND " +
                FIELD_INTERVAL_LAST_UPDATE_TIME + " =:previousLastUpdateTime AND " +
                FIELD_INTERVAL_LAST_UPDATE_DATE + " =:previousLastUpdateDate")
    CompletableFuture<AsyncResultSet> setIntervalProcessed(String book, LocalDate intervalDate, LocalTime intervalStartTime, LocalTime lastUpdateTime, LocalDate lastUpdateDate, boolean processed, boolean previousProcessed, LocalTime previousLastUpdateTime, LocalDate previousLastUpdateDate, String crawlerName, String crawlerVersion, String crawlerType, Function<BoundStatementBuilder, BoundStatementBuilder> attributes);
}

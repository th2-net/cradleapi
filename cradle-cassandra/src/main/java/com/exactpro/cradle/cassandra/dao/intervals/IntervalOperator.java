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

package com.exactpro.cradle.cassandra.dao.intervals;

import com.datastax.oss.driver.api.core.MappedAsyncPagingIterable;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
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
public interface IntervalOperator {
    @Insert(ifNotExists = true)
    CompletableFuture<IntervalEntity> writeInterval(IntervalEntity IntervalEntity, Function<BoundStatementBuilder, BoundStatementBuilder> attributes);

    @Query("SELECT * FROM ${qualifiedTableId} WHERE "+INSTANCE_ID+"=:instanceId AND "+ INTERVAL_DATE +"=:intervalDate AND "+CRAWLER_NAME+"=:crawlerName AND "+CRAWLER_VERSION+"=:crawlerVersion AND "+INTERVAL_START_TIME+">=:intervalStartTime AND "+INTERVAL_START_TIME+"<=:intervalEndTime")
    CompletableFuture<MappedAsyncPagingIterable<IntervalEntity>> getIntervals(UUID instanceId, LocalDate intervalDate, LocalTime intervalStartTime, LocalTime intervalEndTime, String crawlerName, String crawlerVersion, Function<BoundStatementBuilder, BoundStatementBuilder> attributes);

    @Query("UPDATE ${qualifiedTableId} SET "+INTERVAL_LAST_UPDATE_TIME+"=:lastUpdateTime, "+INTERVAL_LAST_UPDATE_DATE+"=:lastUpdateDate WHERE "+INSTANCE_ID+"=:instanceId AND "+INTERVAL_ID+"=:id AND "+CRAWLER_NAME+"=:crawlerName AND "+CRAWLER_VERSION+"=:crawlerVersion AND "+ INTERVAL_DATE +"=:intervalDate AND "+INTERVAL_START_TIME+"=:intervalStartTime IF "+INTERVAL_LAST_UPDATE_TIME+"=:previousLastUpdateTime AND "+INTERVAL_LAST_UPDATE_DATE+"=:previousLastUpdateDate")
    CompletableFuture<AsyncResultSet> setIntervalLastUpdateTimeAndDate(UUID instanceId, String id, LocalDate intervalDate, LocalTime intervalStartTime, LocalTime lastUpdateTime, LocalDate lastUpdateDate, LocalTime previousLastUpdateTime, LocalDate previousLastUpdateDate, String crawlerName, String crawlerVersion, Function<BoundStatementBuilder, BoundStatementBuilder> attributes);

    @Query("UPDATE ${qualifiedTableId} SET "+RECOVERY_STATE_JSON+"=:recoveryStateJson WHERE "+INSTANCE_ID+"=:instanceId AND "+INTERVAL_ID+"=:id AND "+ INTERVAL_DATE +"=:intervalDate AND "+INTERVAL_START_TIME+"=:intervalStartTime AND "+CRAWLER_NAME+"=:crawlerName AND "+CRAWLER_VERSION+"=:crawlerVersion")
    CompletableFuture<AsyncResultSet> updateRecoveryState(UUID instanceId, LocalDate intervalDate, LocalTime intervalStartTime, String id, String crawlerName, String crawlerVersion, String recoveryStateJson);
}

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

package com.exactpro.cradle.cassandra.dao.healing;

import com.datastax.oss.driver.api.core.MappedAsyncPagingIterable;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.annotations.Insert;
import com.datastax.oss.driver.api.mapper.annotations.Query;

import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static com.exactpro.cradle.cassandra.StorageConstants.CRAWLER_TYPE;
import static com.exactpro.cradle.cassandra.StorageConstants.HEALED_EVENT_IDS;
import static com.exactpro.cradle.cassandra.StorageConstants.HEALING_INTERVAL_DATE;
import static com.exactpro.cradle.cassandra.StorageConstants.HEALING_INTERVAL_LAST_UPDATE_DATE;
import static com.exactpro.cradle.cassandra.StorageConstants.HEALING_INTERVAL_LAST_UPDATE_TIME;
import static com.exactpro.cradle.cassandra.StorageConstants.HEALING_INTERVAL_START_TIME;
import static com.exactpro.cradle.cassandra.StorageConstants.INSTANCE_ID;
import static com.exactpro.cradle.cassandra.StorageConstants.RECOVERY_STATE_JSON;

@Dao
public interface HealingIntervalOperator
{
    @Insert(ifNotExists = true)
    CompletableFuture<HealingIntervalEntity> writeHealingInterval(HealingIntervalEntity healingIntervalEntity, Function<BoundStatementBuilder, BoundStatementBuilder> attributes);

    @Query("SELECT * FROM ${qualifiedTableId} WHERE "+INSTANCE_ID+"=:instanceId AND "+HEALING_INTERVAL_DATE+"=:healingIntervalStartDate AND "+CRAWLER_TYPE+"=:crawlerType AND "+HEALING_INTERVAL_START_TIME+">=:healingIntervalStartTime")
    CompletableFuture<MappedAsyncPagingIterable<HealingIntervalEntity>> getHealingIntervals(UUID instanceId, LocalDate healingIntervalStartDate, LocalTime healingIntervalStartTime, String crawlerType, Function<BoundStatementBuilder, BoundStatementBuilder> attributes);

    //FIXME: rename and add word "interval"
    @Query("UPDATE ${qualifiedTableId} SET "+HEALING_INTERVAL_LAST_UPDATE_TIME+"=:lastUpdateTime, "+HEALING_INTERVAL_LAST_UPDATE_DATE+"=toDate(now()) WHERE "+INSTANCE_ID+"=:instanceId AND "+HEALING_INTERVAL_DATE+"=:healingIntervalStartDate AND "+CRAWLER_TYPE+"=:crawlerType AND "+HEALING_INTERVAL_START_TIME+"=:healingIntervalStartTime IF "+HEALING_INTERVAL_LAST_UPDATE_TIME+"=:previousLastUpdateTime AND "+HEALING_INTERVAL_LAST_UPDATE_DATE+"=:previousLastUpdateDate")
    CompletableFuture<AsyncResultSet> setLastUpdateTimeAndDate(UUID instanceId, LocalDate healingIntervalStartDate, LocalTime healingIntervalStartTime, LocalTime lastUpdateTime, LocalTime previousLastUpdateTime, LocalDate previousLastUpdateDate, String crawlerType, Function<BoundStatementBuilder, BoundStatementBuilder> attributes);

    @Query("UPDATE ${qualifiedTableId} SET "+RECOVERY_STATE_JSON+"=:recoveryStateJson WHERE "+INSTANCE_ID+"=:instanceId AND "+HEALING_INTERVAL_DATE+"=:healingIntervalStartDate AND "+CRAWLER_TYPE+"=:crawlerType AND "+HEALING_INTERVAL_START_TIME+"=:healingIntervalStartTime")
    CompletableFuture<AsyncResultSet> updateRecoveryState(UUID instanceId, String recoveryStateJson, LocalDate healingIntervalStartDate, LocalTime healingIntervalStartTime, String crawlerType);

    @Query("UPDATE ${qualifiedTableId} SET "+HEALED_EVENT_IDS+"="+HEALED_EVENT_IDS+" + :healedEventIds WHERE "+INSTANCE_ID+"=:instanceId AND "+HEALING_INTERVAL_DATE+"=:healingIntervalStartDate AND "+CRAWLER_TYPE+"=:crawlerType AND "+HEALING_INTERVAL_START_TIME+"=:healingIntervalStartTime")
    CompletableFuture<AsyncResultSet> storeHealedEventsIds(UUID instanceId, LocalDate healingIntervalStartDate, LocalTime healingIntervalStartTime, String crawlerType, Set<String> healedEventIds);
}

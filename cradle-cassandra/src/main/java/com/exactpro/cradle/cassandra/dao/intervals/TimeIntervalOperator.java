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
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static com.exactpro.cradle.cassandra.StorageConstants.CRAWLER_TYPE;
import static com.exactpro.cradle.cassandra.StorageConstants.HEALED_EVENT_IDS;
import static com.exactpro.cradle.cassandra.StorageConstants.INTERVAL_DATE;
import static com.exactpro.cradle.cassandra.StorageConstants.INTERVAL_LAST_UPDATE_DATE;
import static com.exactpro.cradle.cassandra.StorageConstants.INTERVAL_LAST_UPDATE_TIME;
import static com.exactpro.cradle.cassandra.StorageConstants.INTERVAL_START_TIME;
import static com.exactpro.cradle.cassandra.StorageConstants.INSTANCE_ID;
import static com.exactpro.cradle.cassandra.StorageConstants.RECOVERY_STATE_JSON;

@Dao
public interface TimeIntervalOperator
{
    @Query("SELECT * FROM ${qualifiedTableId} WHERE "+INSTANCE_ID+"=:instanceId AND "+ INTERVAL_DATE +"=:healingIntervalStartDate AND "+CRAWLER_TYPE+"=:crawlerType AND "+ INTERVAL_START_TIME +">=:healingIntervalStartTime")
    CompletableFuture<MappedAsyncPagingIterable<TimeIntervalEntity>> getTimeIntervals(UUID instanceId, LocalDate healingIntervalStartDate, LocalTime healingIntervalStartTime, String crawlerType, Function<BoundStatementBuilder, BoundStatementBuilder> attributes);
}

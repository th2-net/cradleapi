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
package com.exactpro.cradle.cassandra;

import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.exactpro.cradle.cassandra.dao.testevents.EventBatchMaxDurationEntity;
import com.exactpro.cradle.cassandra.dao.testevents.EventBatchMaxDurationOperator;
import com.exactpro.cradle.utils.CradleStorageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

public class EventBatchDurationWorker {

    private final Logger logger = LoggerFactory.getLogger(EventBatchDurationWorker.class);

    private final EventBatchMaxDurationOperator operator;
    private final Function<BoundStatementBuilder, BoundStatementBuilder> readAttrs;
    private final Function<BoundStatementBuilder, BoundStatementBuilder> writeAttrs;
    private final long defaultBatchDurationMillis;
    private final EventBatchDurationCache cache;


    public EventBatchDurationWorker(
            EventBatchDurationCache cache,
            EventBatchMaxDurationOperator operator,
            Function<BoundStatementBuilder, BoundStatementBuilder> readAttrs,
            Function<BoundStatementBuilder, BoundStatementBuilder> writeAttrs,
            long defaultBatchDurationMillis) {
        this.cache = cache;
        this.operator = operator;
        this.readAttrs = readAttrs;
        this.writeAttrs = writeAttrs;
        this.defaultBatchDurationMillis = defaultBatchDurationMillis;
    }

    public CompletableFuture<Void> updateMaxDuration(UUID instanceId, LocalDate date, long duration) throws CradleStorageException {

        EventBatchDurationCache.CacheKey key = new EventBatchDurationCache.CacheKey(instanceId, date);
        Long cachedDuration = cache.getMaxDuration(key);
        var entity = new EventBatchMaxDurationEntity(key.uuid, key.date, duration);
        CompletableFuture<Void> completedFuture = CompletableFuture.completedFuture(null);

        if (cachedDuration != null) {
            if (cachedDuration < duration) {
                // we have already persisted some duration before as we have some value in cache,
                // so we can just update the record in the database
                return operator.updateMaxDuration(entity, duration, writeAttrs)
                        .whenComplete((updated, e) -> {
                            if (e == null && updated) {
                                cache.updateCache(key, duration);
                            }
                        }).thenCompose(res -> completedFuture);
            } else {
                return completedFuture;
            }
        }

        // we don't have any duration cached, so we don't know if record exists in database
        // first try to insert and if no success then try to update
        return operator.writeMaxDuration(entity, writeAttrs)
                .thenCompose((inserted) -> CompletableFuture.supplyAsync(() -> {
                    if (!inserted) {
                        try {
                            return operator.updateMaxDuration(entity, duration, writeAttrs).get();
                        } catch (InterruptedException | ExecutionException e) {
                            logger.warn("Could not update max duration for ({}, {})", entity.getInstanceId(), entity.getStartDate());
                        }
                    }

                    return true;
                }))
            .whenComplete((updated, e) -> {
                if (e == null && updated) {
                    cache.updateCache(key, duration);
                }
            }).thenCompose(e -> completedFuture);
    }

    public long getMaxDuration(UUID instanceId, LocalDate date) {
        EventBatchDurationCache.CacheKey key = new EventBatchDurationCache.CacheKey(instanceId, date);
        EventBatchMaxDurationEntity entity = operator.getMaxDuration(key.uuid, key.date, readAttrs);

        if (entity == null) {
            logger.trace("Could not get max duration for key ({}, {}), returning default value", key.uuid, key.date);

            return defaultBatchDurationMillis;
        }

        return entity.getMaxBatchDuration();
    }
} 
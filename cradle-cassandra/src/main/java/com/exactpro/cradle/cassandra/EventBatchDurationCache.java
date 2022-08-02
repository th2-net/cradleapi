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
import com.datastax.oss.driver.shaded.guava.common.cache.Cache;
import com.datastax.oss.driver.shaded.guava.common.cache.CacheBuilder;
import com.exactpro.cradle.cassandra.dao.testevents.EventBatchMaxDurationOperator;
import com.exactpro.cradle.cassandra.dao.testevents.EventBatchMaxDurationEntity;
import com.exactpro.cradle.utils.CradleStorageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

public class EventBatchDurationCache {

    private final Logger logger = LoggerFactory.getLogger(EventBatchDurationCache.class);

    public static class CacheKey {
        private final UUID uuid;
        private final LocalDate date;

        public  CacheKey (UUID uuid, LocalDate date) {
            this.uuid = uuid;
            this.date = date;
        }

        public UUID getUuid() {
            return uuid;
        }

        public LocalDate getDate() {
            return date;
        }
    }

    private final Cache<CacheKey, Long> durationsCache;
    private final EventBatchMaxDurationOperator operator;
    private final Function<BoundStatementBuilder, BoundStatementBuilder> readAttrs;
    private final Function<BoundStatementBuilder, BoundStatementBuilder> writeAttrs;
    private final long defaultBatchDurationMillis;


    public EventBatchDurationCache(
            EventBatchMaxDurationOperator operator,
            Function<BoundStatementBuilder, BoundStatementBuilder> readAttrs,
            Function<BoundStatementBuilder, BoundStatementBuilder> writeAttrs,
            int limit,
            long defaultBatchDurationMillis) {
        this.operator = operator;
        this.readAttrs = readAttrs;
        this.writeAttrs = writeAttrs;
        this.durationsCache = CacheBuilder.newBuilder().maximumSize(limit).build();
        this.defaultBatchDurationMillis = defaultBatchDurationMillis;
    }

    public CompletableFuture<Void> updateMaxDuration(CacheKey key, long duration) throws CradleStorageException {
        synchronized (durationsCache) {
            Long cached = durationsCache.getIfPresent(key);

            if (cached != null) {
                if (cached > duration) {
                    return CompletableFuture.completedFuture(null);
                }
            }
        }


        return operator.writeMaxDuration(key.getUuid(), key.getDate(), duration, writeAttrs)
                .thenAcceptAsync((res) -> operator.updateMaxDuration(key.getUuid(), key.getDate(), duration, duration, writeAttrs))
                .whenComplete((rtn, e) -> {
                    if (e != null) {
                        synchronized (durationsCache) {
                            Long cached = durationsCache.getIfPresent(key);

                            if (cached != null) {
                                // cache might be updated by other threads, need to check again
                                if (cached > duration) {
                                    return;
                                }
                            }

                            durationsCache.put(key, duration);
                        }
                    }
                });
    }

    public long getMaxDuration(CacheKey key) {
        EventBatchMaxDurationEntity entity = operator.getMaxDuration(key.getUuid(), key.getDate(), readAttrs);

        if (entity == null) {
            logger.trace("Could not get max duration for key ({}, {}), returning default value", key.getUuid(), key.getDate());

            return defaultBatchDurationMillis;
        }

        return entity.getMaxBatchDuration();
    }
}
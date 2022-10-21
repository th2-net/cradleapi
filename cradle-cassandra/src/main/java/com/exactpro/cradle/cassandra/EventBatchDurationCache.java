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

import com.datastax.oss.driver.shaded.guava.common.cache.Cache;
import com.datastax.oss.driver.shaded.guava.common.cache.CacheBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.util.Objects;
import java.util.UUID;

public class EventBatchDurationCache {

    private final Logger logger = LoggerFactory.getLogger(EventBatchDurationCache.class);

    public static class CacheKey {
        private final UUID uuid;
        private final LocalDate date;

        public  CacheKey (UUID uuid, LocalDate date) {
            this.uuid = uuid;
            this.date = date;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof CacheKey)) return false;
            CacheKey key = (CacheKey) o;
            return Objects.equals(uuid, key.uuid) && Objects.equals(date, key.date);
        }

        @Override
        public int hashCode() {
            return Objects.hash(uuid, date);
        }
    }

    private final Cache<CacheKey, Long> durationsCache;

    public EventBatchDurationCache(int limit) {
        this.durationsCache = CacheBuilder.newBuilder().maximumSize(limit).build();
    }

    public Long getMaxDuration (CacheKey cacheKey) {
        synchronized (durationsCache) {
            return durationsCache.getIfPresent(cacheKey);
        }
    }

    public void updateCache(CacheKey key, long duration) {
        synchronized (durationsCache) {
            Long cached = durationsCache.getIfPresent(key);

            if (cached != null) {
                logger.trace("Checking against cached value");
                if (cached > duration) {
                    return;
                }
            }

            logger.trace("Updating cache");
            durationsCache.put(key, duration);
        }
    }
}
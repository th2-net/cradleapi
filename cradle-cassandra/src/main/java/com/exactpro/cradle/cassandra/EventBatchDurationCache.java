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
import com.exactpro.cradle.PageId;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class EventBatchDurationCache {

    public static class CacheKey {
        private final String book;
        private final String page;
        private final String scope;

        public CacheKey(String book, String page, String scope) {
            this.book = book;
            this.page = page;
            this.scope = scope;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof CacheKey)) return false;
            CacheKey key = (CacheKey) o;
            return book.equals(key.book) && page.equals(key.page) && scope.equals(key.scope);
        }

        @Override
        public int hashCode() {
            return Objects.hash(book, page, scope);
        }
    }

    private final Cache<CacheKey, Long> durationsCache;


    public EventBatchDurationCache(int limit) {
        this.durationsCache = CacheBuilder.newBuilder().maximumSize(limit).build();
    }

    public Long getMaxDuration (String book, String page, String scope) {
        CacheKey key = new CacheKey(book, page, scope);
        synchronized (durationsCache) {
            return durationsCache.getIfPresent(key);
        }
    }

    public void updateCache(String book, String page, String scope, long duration) {
        CacheKey key = new CacheKey(book, page, scope);
        synchronized (durationsCache) {
            Long cached = durationsCache.getIfPresent(key);

            if (cached != null) {
                if (cached > duration) {
                    return;
                }
            }

            durationsCache.put(key, duration);
        }
    }



    public int removePageDurations (PageId pageId) {
        List<CacheKey> keysToRemove = new ArrayList<>();

        // Remove from cache
        synchronized (durationsCache) {
            for (CacheKey key : durationsCache.asMap().keySet()) {
                if (key.book.equals(pageId.getBookId().getName())&& key.page.equals(pageId.getName())) {
                    keysToRemove.add(key);
                }
            }

            durationsCache.invalidateAll(keysToRemove);
        }

        return keysToRemove.size();
    }
}
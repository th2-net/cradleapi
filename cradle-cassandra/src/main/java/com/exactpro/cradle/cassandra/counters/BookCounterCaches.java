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
package com.exactpro.cradle.cassandra.counters;

import com.exactpro.cradle.BookId;
import com.exactpro.cradle.EntityType;
import org.apache.commons.lang3.builder.EqualsBuilder;

import java.util.*;

public class BookCounterCaches {

    private final BookId bookId;
    private final MessageCounterCache messageCounterCache;
    private final EntityCounterCache entityCounterCache;

    public BookCounterCaches(BookId bookId) {
        this.bookId = bookId;
        messageCounterCache = new MessageCounterCache();
        entityCounterCache = new EntityCounterCache();
    }

    public BookId getBookId() {
        return bookId;
    }

    public MessageCounterCache getMessageCounterCache() {
        return messageCounterCache;
    }

    public EntityCounterCache getEntityCounterCache() {
        return entityCounterCache;
    }

    public static class MessageKey {
        private final String page;
        private final String sessionAlias;
        private final String direction;
        public MessageKey(String page,String sessionAlias, String direction) {
            this.page = page;
            this.sessionAlias = sessionAlias;
            this.direction = direction;
        }

        public String getPage() { return page; }
        public String getSessionAlias() {
            return sessionAlias;
        }

        public String getDirection() {
            return direction;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            MessageKey that = (MessageKey) o;
            return Objects.equals(page, that.page) && Objects.equals(sessionAlias, that.sessionAlias) && Objects.equals(direction, that.direction);
        }

        @Override
        public int hashCode() {
            return Objects.hash(page, sessionAlias, direction);
        }
    }

    public static class MessageCounterCache {
        private final Map<MessageKey, CounterCache> cache = new HashMap<>();
        public synchronized void put(MessageKey key, CounterCache counters) {
            cache.put(key, counters);
        }
        public synchronized CounterCache get(MessageKey key) {
            return cache.computeIfAbsent(key, k -> new CounterCache());
        }
        public synchronized CounterCache extract(MessageKey key) {
            CounterCache result = cache.get(key);
            cache.remove(key);
            return result;
        }
        public synchronized Collection<MessageKey> messageKeys() {
            return new HashSet<>(cache.keySet());
        }
    }

    public static class EntityCounterCache {
        private final Map<EntityType, CounterCache> cache;
        public EntityCounterCache() {
            cache = new HashMap<>();
            for (EntityType t : EntityType.values())
                cache.put(t, new CounterCache());
        }
        public CounterCache forEntityType(EntityType entityType) {
            return cache.get(entityType);
        }
    }
}

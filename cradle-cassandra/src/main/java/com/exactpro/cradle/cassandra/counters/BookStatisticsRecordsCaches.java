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
import com.exactpro.cradle.SessionRecordType;
import com.exactpro.cradle.counters.Counter;

import java.util.*;

public class BookStatisticsRecordsCaches {

    private final BookId bookId;
    private final MessageCounterCache messageCounterCache;
    private final EntityCounterCache entityCounterCache;
    private final SessionRecordCache sessionRecordCache;

    public BookStatisticsRecordsCaches(BookId bookId) {
        this.bookId = bookId;
        messageCounterCache = new MessageCounterCache();
        entityCounterCache = new EntityCounterCache();
        sessionRecordCache = new SessionRecordCache();
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

    public SessionRecordCache getSessionRecordCache() {
        return sessionRecordCache;
    }

    public boolean notEmpty() {
        return !(messageCounterCache.isEmpty() && entityCounterCache.isEmpty() && sessionRecordCache.isEmpty());
    }

    public interface RecordKey {}

    public static class SessionRecordKey implements RecordKey {
        private final String page;
        private final SessionRecordType recordType;
        public SessionRecordKey(String page, SessionRecordType recordType) {
            this.page = page;
            this.recordType = recordType;
        }

        public String getPage() {
            return page;
        }

        public SessionRecordType getRecordType() {
            return recordType;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            SessionRecordKey that = (SessionRecordKey) o;

            if (!page.equals(that.page)) return false;
            return recordType == that.recordType;
        }

        @Override
        public int hashCode() {
            int result = page.hashCode();
            result = 31 * result + recordType.hashCode();
            return result;
        }
    }


    public static class MessageKey implements RecordKey {
        private final String page;
        private final String sessionAlias;
        private final String direction;
        public MessageKey(String page, String sessionAlias, String direction) {
            this.page = page;
            this.sessionAlias = sessionAlias;
            this.direction = direction;
        }

        public String getPage() {
            return page;
        }

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

    public static class EntityKey implements RecordKey {
        private final String page;
        private final EntityType entityType;

        public EntityKey(String page, EntityType entityType){
            this.page = page;
            this.entityType = entityType;
        }
        public String getPage() { return page; }
        public EntityType getEntityType() { return entityType; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            EntityKey entityKey = (EntityKey) o;
            return Objects.equals(page, entityKey.page) && entityType == entityKey.entityType;
        }

        @Override
        public int hashCode() {
            return Objects.hash(page, entityType);
        }
    }


    public static class MessageCounterCache extends RecordCache<MessageKey, Counter> {
        public MessageCounterCache() {
            super(new CounterTimeFrameRecordFactory());
        }
    }

    public static class SessionRecordCache extends RecordCache<SessionRecordKey, SessionList> {
        public SessionRecordCache() {
            super(new SessionsTimeFrameRecordFactory());
        }
    }

    public static class EntityCounterCache extends RecordCache<EntityKey, Counter> {
        public EntityCounterCache() { super(new CounterTimeFrameRecordFactory()); }
    }

    public static class RecordCache<K, V> {
        private final Map<K, TimeFrameRecordCache<V>> cache;
        private final TimeFrameRecordFactory<V> recordFactory;

        public RecordCache(TimeFrameRecordFactory<V> recordFactory) {
            this.cache = new HashMap<>();
            this.recordFactory = recordFactory;
        }

        public synchronized Collection<K> getKeys() {
            return new HashSet<>(cache.keySet());
        }

        public synchronized void put(K key, TimeFrameRecordCache<V> recordCache) {
            cache.put(key, recordCache);
        }

        public synchronized TimeFrameRecordCache<V> get(K key) {
            return cache.computeIfAbsent(key, k -> new TimeFrameRecordCache<>(recordFactory));
        }

        public synchronized TimeFrameRecordCache<V> extract(K key) {
            TimeFrameRecordCache<V> result = cache.get(key);
            cache.remove(key);
            return result;
        }

        public synchronized boolean isEmpty() {
            return cache.isEmpty();
        }
    }
}

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
package com.exactpro.cradle.cassandra.workers;

import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.exactpro.cradle.BookId;
import com.exactpro.cradle.Counter;
import com.exactpro.cradle.EntityType;
import com.exactpro.cradle.FrameType;
import com.exactpro.cradle.cassandra.counters.CounterCache;
import com.exactpro.cradle.cassandra.counters.EntityStatisticsCollector;
import com.exactpro.cradle.cassandra.counters.MessageStatisticsCollector;
import com.exactpro.cradle.cassandra.counters.TimeFrameCounter;
import com.exactpro.cradle.cassandra.dao.CradleOperators;
import com.exactpro.cradle.serialization.SerializedEntityMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public class StatisticsWorker implements Runnable, EntityStatisticsCollector, MessageStatisticsCollector {

    private static final Logger logger = LoggerFactory.getLogger(StatisticsWorker.class);

    private final CradleOperators ops;
    private final long interval;
    private final BookEntityCounterCache bookEntityCounterCache;
    private final BookMessageCounterCache bookMessageCounterCache;
    private final Function<BoundStatementBuilder, BoundStatementBuilder> writeAttrs;

    public StatisticsWorker(CradleOperators ops, Function<BoundStatementBuilder, BoundStatementBuilder> writeAttrs, long persistanceInterval) {
        this.ops = ops;
        this.writeAttrs = writeAttrs;
        this.interval = persistanceInterval;
        this.bookMessageCounterCache = new BookMessageCounterCache();
        this.bookEntityCounterCache = new BookEntityCounterCache();
    }


    private EntityCounterCache createEntityCounters() {
        EntityCounterCache entityCounters = new EntityCounterCache();
        for (EntityType t : EntityType.values())
            entityCounters.put(t, new CounterCache());
        return entityCounters;
    }

    private MessageCounterCache createMessageCounters() {
        return new MessageCounterCache();
    }


    private ScheduledExecutorService executorService;
    public void start() {
        if (interval == 0) {
            logger.info("Counter persistence service is disabled");
            return;
        }

        logger.info("Starting executor for StatisticsWorker");
        executorService = Executors.newScheduledThreadPool(1);
        executorService.scheduleAtFixedRate(this, 0, interval, TimeUnit.MILLISECONDS);
        logger.info("StatisticsWorker executor started");
    }

    public void stop() {
        if (interval == 0)
            return;

        if (executorService == null)
            throw new IllegalStateException("Can not stop statistics worker as it is not started");
        logger.info("Shutting down StatisticsWorker executor");
        executorService.shutdown();
        try {
            logger.debug("Waiting StatisticsWorker jobs to complete");
            executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
            logger.debug("StatisticsWorker shutdown complete");
        } catch (InterruptedException e) {
            logger.error("Interrupted while waiting jobs to complete");
        }
    }


    @Override
    public void updateEntityBatchStatistics(BookId bookId, EntityType entityType, Collection<SerializedEntityMetadata> batchMetadata) {

        EntityCounterCache entityCounters = bookEntityCounterCache.computeIfAbsent(bookId, k -> createEntityCounters());
        CounterCache counters = entityCounters.get(entityType);
        batchMetadata.forEach(meta -> {
            Counter counter = new Counter(1, meta.getSerializedEntitySize());
            for (FrameType t : FrameType.values()) {
                counters.getCounterSamples(t).update(meta.getTimestamp(), counter);
            }
        });
    }

    @Override
    public void updateMessageBatchStatistics(BookId bookId, String sessionAlias, String direction, Collection<SerializedEntityMetadata> batchMetadata) {

        MessageCounterCache messageCounters = bookMessageCounterCache.computeIfAbsent(bookId, k -> createMessageCounters());
        MessageKey key = new MessageKey(sessionAlias, direction);
        CounterCache counters = messageCounters.computeIfAbsent(key, k -> new CounterCache());
        batchMetadata.forEach(meta -> {
            Counter counter = new Counter(1, meta.getSerializedEntitySize());
            for (FrameType t : FrameType.values()) {
                counters.getCounterSamples(t).update(meta.getTimestamp(), counter);
            }
        });

        // update entity statistics separately
        updateEntityBatchStatistics(bookId, EntityType.MESSAGE, batchMetadata);
    }

    private void persistEntityCounters(BookId bookId, EntityType entityType, FrameType frameType, TimeFrameCounter counter) {
        try {
            logger.trace("Persisting counter for {}:{}:{}:{}", bookId, entityType, frameType, counter.getFrameStart());
            ops.getOperators(bookId).getEntityStatisticsOperator().update(
                    entityType.getValue(),
                    frameType.getValue(),
                    counter.getFrameStart(),
                    counter.getCounter().getEntityCount(),
                    counter.getCounter().getEntitySize(),
                    writeAttrs
            );
        } catch (Exception e) {
            logger.error("Exception persisting counter, retry scheduled", e);
            EntityCounterCache entityCounters = bookEntityCounterCache.get(bookId);
            entityCounters.get(entityType).getCounterSamples(frameType).update(counter.getFrameStart(), counter.getCounter());
        }
    }


    private void persistMessageCounters(BookId bookId, MessageKey key, FrameType frameType, TimeFrameCounter counter) {
        try {
            logger.trace("Persisting counter for {}:{}:{}:{}:{}", bookId, key.getSessionAlias(), key.getDirection(), frameType, counter.getFrameStart());
            ops.getOperators(bookId).getMessageStatisticsOperator().update(
                    key.getSessionAlias(),
                    key.getDirection(),
                    frameType.getValue(),
                    counter.getFrameStart(),
                    counter.getCounter().getEntityCount(),
                    counter.getCounter().getEntitySize(),
                    writeAttrs
            );
        } catch (Exception e) {
            logger.error("Exception persisting counter, retry scheduled", e);
            MessageCounterCache entityCounters = bookMessageCounterCache.get(bookId);
            entityCounters.get(key).getCounterSamples(frameType).update(counter.getFrameStart(), counter.getCounter());
        }
    }

    @Override
    public void run() {
        logger.trace("executing StatisticsWorker job");

        try {
            for (BookId bookId: bookEntityCounterCache.keySet()) {

                // persist all cached entity counters
                EntityCounterCache entityCounters = bookEntityCounterCache.get(bookId);
                for (FrameType frameType : FrameType.values()) {
                    for (EntityType entityType : EntityType.values()) {
                        Collection<TimeFrameCounter> counters = entityCounters.get(entityType).getCounterSamples(frameType).extractAll();
                        counters.forEach(counter -> persistEntityCounters(bookId, entityType, frameType, counter));
                    }
                }

                // persist all cached message counters
                MessageCounterCache messageCounters = bookMessageCounterCache.get(bookId);
                for (MessageKey key :messageCounters.keySet()) {
                    CounterCache counterCache = messageCounters.get(key);
                    for (FrameType frameType : FrameType.values()) {
                        Collection<TimeFrameCounter> counters = counterCache.getCounterSamples(frameType).extractAll();
                        counters.forEach(counter -> persistMessageCounters(bookId, key, frameType, counter));
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Exception processing job", e);
        }

        logger.trace("StatisticsWorker job complete");
    }

    private static class MessageCounterCache extends HashMap<MessageKey, CounterCache> {}
    private static class EntityCounterCache extends HashMap<EntityType, CounterCache> {}
    private static class BookMessageCounterCache extends ConcurrentHashMap<BookId, MessageCounterCache> {}
    private static class BookEntityCounterCache extends ConcurrentHashMap<BookId, EntityCounterCache> {}
    private static class MessageKey {
        private final String sessionAlias;
        private final String direction;
        public MessageKey(String sessionAlias, String direction) {
            this.sessionAlias = sessionAlias;
            this.direction = direction;
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

            if (sessionAlias != null ? !sessionAlias.equals(that.sessionAlias) : that.sessionAlias != null)
                return false;
            return direction != null ? direction.equals(that.direction) : that.direction == null;
        }

        @Override
        public int hashCode() {
            int result = sessionAlias != null ? sessionAlias.hashCode() : 0;
            result = 31 * result + (direction != null ? direction.hashCode() : 0);
            return result;
        }
    }
}
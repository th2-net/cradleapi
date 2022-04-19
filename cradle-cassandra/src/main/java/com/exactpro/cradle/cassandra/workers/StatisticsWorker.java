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
import com.exactpro.cradle.counters.Counter;
import com.exactpro.cradle.EntityType;
import com.exactpro.cradle.FrameType;
import com.exactpro.cradle.cassandra.counters.*;
import com.exactpro.cradle.cassandra.dao.CradleOperators;
import com.exactpro.cradle.serialization.SerializedEntityMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;

public class StatisticsWorker implements Runnable, EntityStatisticsCollector, MessageStatisticsCollector {

    private static final Logger logger = LoggerFactory.getLogger(StatisticsWorker.class);

    private final CradleOperators ops;
    private final long interval;
    private final Map<BookId, BookCounterCaches> bookCounterCaches;
    private final Function<BoundStatementBuilder, BoundStatementBuilder> writeAttrs;

    public StatisticsWorker(CradleOperators ops, Function<BoundStatementBuilder, BoundStatementBuilder> writeAttrs, long persistanceInterval) {
        this.ops = ops;
        this.writeAttrs = writeAttrs;
        this.interval = persistanceInterval;
        this.bookCounterCaches = new ConcurrentHashMap<>();
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


    private BookCounterCaches getBookCounterCaches(BookId bookId) {
        return bookCounterCaches.computeIfAbsent(bookId, k -> new BookCounterCaches(bookId));
    }


    private void updateCounters(CounterCache counters, Collection<SerializedEntityMetadata> batchMetadata) {
        batchMetadata.forEach(meta -> {
            Counter counter = new Counter(1, meta.getSerializedEntitySize());
            for (FrameType t : FrameType.values()) {
                counters.getCounterSamples(t).update(meta.getTimestamp(), counter);
            }
        });
    }


    @Override
    public void updateEntityBatchStatistics(BookId bookId, EntityType entityType, Collection<SerializedEntityMetadata> batchMetadata) {
        CounterCache counters = getBookCounterCaches(bookId)
                .getEntityCounterCache()
                .forEntityType(entityType);
        updateCounters(counters, batchMetadata);
    }


    @Override
    public void updateMessageBatchStatistics(BookId bookId,String page, String sessionAlias, String direction, Collection<SerializedEntityMetadata> batchMetadata) {
        CounterCache counters = getBookCounterCaches(bookId)
                .getMessageCounterCache()
                .get(new BookCounterCaches.MessageKey(page, sessionAlias, direction));
        updateCounters(counters, batchMetadata);

        // update entity statistics separately
        updateEntityBatchStatistics(bookId, EntityType.MESSAGE, batchMetadata);
    }


    private void persistEntityCounters(BookId bookId, EntityType entityType, FrameType frameType, TimeFrameCounter counter) {

        final Consumer<Throwable> exceptionHandler = (Throwable e) -> {
            logger.error("Exception persisting entity counter, rescheduling", e);
            getBookCounterCaches(bookId)
                .getEntityCounterCache()
                .forEntityType(entityType)
                .getCounterSamples(frameType)
                .update(counter.getFrameStart(), counter.getCounter());
        };

        try {
            logger.trace("Persisting entity counter for {}:{}:{}:{}", bookId, entityType, frameType, counter.getFrameStart());
            ops.getOperators(bookId).getEntityStatisticsOperator().update(
                    entityType.getValue(),
                    frameType.getValue(),
                    counter.getFrameStart(),
                    counter.getCounter().getEntityCount(),
                    counter.getCounter().getEntitySize(),
                    writeAttrs
            ).whenComplete((result, e) -> {
                if (e != null)
                    exceptionHandler.accept(e);
            });
        } catch (Exception e) {
            exceptionHandler.accept(e);
        }
    }


    private void persistMessageCounters(BookId bookId, BookCounterCaches.MessageKey key, FrameType frameType, TimeFrameCounter counter) {

        final Consumer<Throwable> exceptionHandler = (Throwable e) -> {
            logger.error("Exception persisting message counter, rescheduling", e);
            getBookCounterCaches(bookId)
                .getMessageCounterCache()
                .get(key)
                .getCounterSamples(frameType)
                .update(counter.getFrameStart(), counter.getCounter());
        };

        try {
            logger.trace("Persisting message counter for {}:{}:{}:{}:{}", bookId, key.getSessionAlias(), key.getDirection(), frameType, counter.getFrameStart());
            ops.getOperators(bookId).getMessageStatisticsOperator().update(
                    key.getPage(),
                    key.getSessionAlias(),
                    key.getDirection(),
                    frameType.getValue(),
                    counter.getFrameStart(),
                    counter.getCounter().getEntityCount(),
                    counter.getCounter().getEntitySize(),
                    writeAttrs
            ).whenComplete((result, e) -> {
                if (e != null)
                    exceptionHandler.accept(e);
            });
        } catch (Exception e) {
            exceptionHandler.accept(e);
        }
    }

    @Override
    public void run() {
        logger.trace("executing StatisticsWorker job");

        try {
            for (BookId bookId: bookCounterCaches.keySet()) {

                BookCounterCaches bookCounterCaches = getBookCounterCaches(bookId);

                // persist all cached entity counters
                BookCounterCaches.EntityCounterCache entityCounters = bookCounterCaches.getEntityCounterCache();
                for (FrameType frameType : FrameType.values()) {
                    for (EntityType entityType : EntityType.values()) {
                        Collection<TimeFrameCounter> counters = entityCounters.forEntityType(entityType).getCounterSamples(frameType).extractAll();
                        counters.forEach(counter -> persistEntityCounters(bookId, entityType, frameType, counter));
                    }
                }

                // persist all cached message counters
                BookCounterCaches.MessageCounterCache messageCounters = bookCounterCaches.getMessageCounterCache();
                Collection<BookCounterCaches.MessageKey> messageKeys = messageCounters.messageKeys();
                for (BookCounterCaches.MessageKey key : messageKeys) {
                    CounterCache counterCache = messageCounters.extract(key);
                    if (counterCache == null)
                        continue;
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
}
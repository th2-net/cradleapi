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
import com.exactpro.cradle.EntityType;
import com.exactpro.cradle.FrameType;
import com.exactpro.cradle.SessionRecordType;
import com.exactpro.cradle.cassandra.counters.*;
import com.exactpro.cradle.cassandra.dao.CradleOperators;
import com.exactpro.cradle.cassandra.dao.SessionStatisticsEntity;
import com.exactpro.cradle.cassandra.dao.SessionStatisticsOperator;
import com.exactpro.cradle.counters.Counter;
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

public class StatisticsWorker implements Runnable, EntityStatisticsCollector, MessageStatisticsCollector, SessionStatisticsCollector {

    private static final Logger logger = LoggerFactory.getLogger(StatisticsWorker.class);

    private final CradleOperators ops;
    private final long interval;
    private final Map<BookId, BookStatisticsRecordsCaches> bookCounterCaches;
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

        // ensure that cache is empty before executor service initiating shutdown
        if (bookCounterCachesNotEmpty()) {
            logger.info("Waiting statistics cache depletion");
            while (bookCounterCachesNotEmpty())
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    logger.error("Interrupted while waiting statistics cache depletion");
                }
        }

        // shut down executor service and wait for current job to complete
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


    private BookStatisticsRecordsCaches getBookStatisticsRecordsCaches(BookId bookId) {
        return bookCounterCaches.computeIfAbsent(bookId, k -> new BookStatisticsRecordsCaches(bookId));
    }


    private boolean bookCounterCachesNotEmpty() {
        return bookCounterCaches.values().stream().anyMatch(BookStatisticsRecordsCaches::notEmpty);
    }


    private void updateCounters(TimeFrameRecordCache<Counter> counters, Collection<SerializedEntityMetadata> batchMetadata) {
        batchMetadata.forEach(meta -> {
            Counter counter = new Counter(1, meta.getSerializedEntitySize());
            for (FrameType t : FrameType.values()) {
                counters.getRecordSamples(t).update(meta.getTimestamp(), counter);
            }
        });
    }


    @Override
    public void updateEntityBatchStatistics(BookId bookId, BookStatisticsRecordsCaches.EntityKey entityKey, Collection<SerializedEntityMetadata> batchMetadata) {
        TimeFrameRecordCache<Counter> counters = getBookStatisticsRecordsCaches(bookId)
                .getEntityCounterCache()
                .get(entityKey);
        updateCounters(counters, batchMetadata);
    }


    @Override
    public void updateMessageBatchStatistics(BookId bookId, String page, String sessionAlias, String direction, Collection<SerializedEntityMetadata> batchMetadata) {
        TimeFrameRecordCache<Counter> counters = getBookStatisticsRecordsCaches(bookId)
                .getMessageCounterCache()
                .get(new BookStatisticsRecordsCaches.MessageKey(page, sessionAlias, direction));
        updateCounters(counters, batchMetadata);
        BookStatisticsRecordsCaches.EntityKey key = new BookStatisticsRecordsCaches.EntityKey(page, EntityType.MESSAGE);
        // update entity statistics separately
        updateEntityBatchStatistics(bookId, key, batchMetadata);
    }

    @Override
    public void updateSessionStatistics(BookId bookId, String page, SessionRecordType recordType, String session, Collection<SerializedEntityMetadata> batchMetadata) {
        TimeFrameRecordCache<SessionList> samples = getBookStatisticsRecordsCaches(bookId)
                .getSessionRecordCache()
                .get(new BookStatisticsRecordsCaches.SessionRecordKey(page, recordType));

        batchMetadata.forEach(meta -> {
            for (FrameType t : FrameType.values()) {
                samples.getRecordSamples(t).update(meta.getTimestamp(), new SessionList(session));
            }
        });
    }


    private void persistEntityCounters(BookId bookId, BookStatisticsRecordsCaches.EntityKey entityKey, FrameType frameType, TimeFrameRecord<Counter> counter) {

        final Consumer<Throwable> exceptionHandler = (Throwable e) -> {
            logger.error("Exception persisting entity counter, rescheduling", e);
            getBookStatisticsRecordsCaches(bookId)
                .getEntityCounterCache()
                .get(entityKey)
                .getRecordSamples(frameType)
                .update(counter.getFrameStart(), counter.getRecord());
        };

        try {
            logger.trace("Persisting entity counter for {}:{}:{}:{}", bookId, entityKey, frameType, counter.getFrameStart());
            ops.getOperators(bookId).getEntityStatisticsOperator().update(bookId.getName(),
                    entityKey.getPage(),
                    entityKey.getEntityType().getValue(),
                    frameType.getValue(),
                    counter.getFrameStart(),
                    counter.getRecord().getEntityCount(),
                    counter.getRecord().getEntitySize(),
                    writeAttrs
            ).whenComplete((result, e) -> {
                if (e != null)
                    exceptionHandler.accept(e);
            });
        } catch (Exception e) {
            exceptionHandler.accept(e);
        }
    }



    private void persistMessageCounters(BookId bookId, BookStatisticsRecordsCaches.MessageKey key, FrameType frameType, TimeFrameRecord<Counter> counter) {

        final Consumer<Throwable> exceptionHandler = (Throwable e) -> {
            logger.error("Exception persisting message counter, rescheduling", e);
            getBookStatisticsRecordsCaches(bookId)
                .getMessageCounterCache()
                .get(key)
                .getRecordSamples(frameType)
                .update(counter.getFrameStart(), counter.getRecord());
        };

        try {
            logger.trace("Persisting message counter for {}:{}:{}:{}:{}", bookId, key.getSessionAlias(), key.getDirection(), frameType, counter.getFrameStart());
            ops.getOperators(bookId).getMessageStatisticsOperator().update(
                    bookId.getName(),
                    key.getPage(),
                    key.getSessionAlias(),
                    key.getDirection(),
                    frameType.getValue(),
                    counter.getFrameStart(),
                    counter.getRecord().getEntityCount(),
                    counter.getRecord().getEntitySize(),
                    writeAttrs
            ).whenComplete((result, e) -> {
                if (e != null)
                    exceptionHandler.accept(e);
            });
        } catch (Exception e) {
            exceptionHandler.accept(e);
        }
    }


    private void persistSessionStatistics(BookId bookId, BookStatisticsRecordsCaches.SessionRecordKey key, FrameType frameType, TimeFrameRecord<SessionList> sessionsRecord) {

        final Consumer<Throwable> exceptionHandler = (Throwable e) -> {
            logger.error("Exception persisting session statistics, rescheduling", e);
            getBookStatisticsRecordsCaches(bookId)
                    .getSessionRecordCache()
                    .get(key)
                    .getRecordSamples(frameType)
                    .update(sessionsRecord.getFrameStart(), sessionsRecord.getRecord());
        };

        try {
            Collection<String> sessions = sessionsRecord.getRecord().getSessions();
            logger.trace("Persisting session statistics for {}:{}:{}:{}:{} with {} records"
                    , bookId
                    , key.getPage()
                    , key.getRecordType()
                    , frameType
                    , sessionsRecord.getFrameStart()
                    , sessions.size());

            SessionStatisticsOperator op = ops.getOperators(bookId).getSessionStatisticsOperator();
            for (String session : sessions) {
                op.write(new SessionStatisticsEntity(
                                key.getPage(),
                                key.getRecordType().getValue(),
                                frameType.getValue(),
                                sessionsRecord.getFrameStart(),
                                session)
                        , writeAttrs)
                        .whenComplete((result, e) -> {
                            if (e != null)
                                exceptionHandler.accept(e);
                        });
            }
        } catch (Exception e) {
            exceptionHandler.accept(e);
        }
    }



    private <K extends BookStatisticsRecordsCaches.RecordKey, V> void processRecords(
            BookId bookId,
            BookStatisticsRecordsCaches.RecordCache<K, V> recordCache,
            Persistor<K, V> persistor)

    {
        Collection<K> keys = recordCache.getKeys();
        for (K key : keys) {
            TimeFrameRecordCache<V> timeFrameRecords = recordCache.extract(key);
            if (timeFrameRecords == null)
                continue;
            for (FrameType frameType : FrameType.values()) {
                Collection<TimeFrameRecord<V>> records = timeFrameRecords.getRecordSamples(frameType).extractAll();
                records.forEach(record -> persistor.persist(bookId, key, frameType, record));
            }
        }
    }


    @Override
    public void run() {
        logger.trace("executing StatisticsWorker job");

        try {
            for (BookId bookId: bookCounterCaches.keySet()) {

                BookStatisticsRecordsCaches bookStatisticsRecordsCaches = getBookStatisticsRecordsCaches(bookId);

                // persist all cached entity counters
                processRecords(bookId, bookStatisticsRecordsCaches.getEntityCounterCache(), this::persistEntityCounters);

                // persist all cached message counters
                processRecords(bookId, bookStatisticsRecordsCaches.getMessageCounterCache(), this::persistMessageCounters);

                // persist all cached session statistics
                processRecords(bookId, bookStatisticsRecordsCaches.getSessionRecordCache(), this::persistSessionStatistics);
            }
        } catch (Exception e) {
            logger.error("Exception processing job", e);
        }

        logger.trace("StatisticsWorker job complete");
    }


    private interface Persistor<K extends BookStatisticsRecordsCaches.RecordKey, V> {
        void persist(BookId bookId, K key, FrameType frameType, TimeFrameRecord<V> record);
    }
}
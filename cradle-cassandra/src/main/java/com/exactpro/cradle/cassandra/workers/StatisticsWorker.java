/*
 * Copyright 2020-2025 Exactpro (Exactpro Systems Limited)
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

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BatchStatementBuilder;
import com.datastax.oss.driver.shaded.guava.common.util.concurrent.ThreadFactoryBuilder;
import com.exactpro.cradle.BookId;
import com.exactpro.cradle.EntityType;
import com.exactpro.cradle.FrameType;
import com.exactpro.cradle.SessionRecordType;
import com.exactpro.cradle.cassandra.counters.BookStatisticsRecordsCaches;
import com.exactpro.cradle.cassandra.counters.EntityStatisticsCollector;
import com.exactpro.cradle.cassandra.counters.MessageStatisticsCollector;
import com.exactpro.cradle.cassandra.counters.SessionList;
import com.exactpro.cradle.cassandra.counters.SessionStatisticsCollector;
import com.exactpro.cradle.cassandra.counters.TimeFrameRecord;
import com.exactpro.cradle.cassandra.counters.TimeFrameRecordCache;
import com.exactpro.cradle.cassandra.counters.TimeFrameRecordSamples;
import com.exactpro.cradle.cassandra.dao.CassandraOperators;
import com.exactpro.cradle.cassandra.dao.statistics.EntityStatisticsEntity;
import com.exactpro.cradle.cassandra.dao.statistics.EntityStatisticsOperator;
import com.exactpro.cradle.cassandra.dao.statistics.MessageStatisticsEntity;
import com.exactpro.cradle.cassandra.dao.statistics.MessageStatisticsOperator;
import com.exactpro.cradle.cassandra.dao.statistics.SessionStatisticsEntity;
import com.exactpro.cradle.cassandra.dao.statistics.SessionStatisticsOperator;
import com.exactpro.cradle.cassandra.utils.LimitedCache;
import com.exactpro.cradle.counters.Counter;
import com.exactpro.cradle.serialization.SerializedEntityMetadata;
import com.exactpro.cradle.serialization.SerializedMessageMetadata;
import com.exactpro.th2.taskutils.FutureTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public class StatisticsWorker implements Runnable, EntityStatisticsCollector, MessageStatisticsCollector, SessionStatisticsCollector {

    private static final Logger LOGGER = LoggerFactory.getLogger(StatisticsWorker.class);
    private static final int MAX_BATCH_STATEMENTS = 16384;

    private static final ThreadFactory THREAD_FACTORY = new ThreadFactoryBuilder().setNameFormat("cradle-statistics-worker-%d").build();

    private final FutureTracker<AsyncResultSet> futures;
    private final CassandraOperators operators;
    private final long interval;
    private final Map<BookId, BookStatisticsRecordsCaches> bookCounterCaches;
    private final Function<BatchStatementBuilder, BatchStatementBuilder> batchWriteAttrs;
    private final boolean isEnabled;

    public StatisticsWorker (WorkerSupplies workerSupplies, long persistenceInterval, int parallel) {
        this.futures = FutureTracker.create(parallel);
        this.operators = workerSupplies.getOperators();
        this.batchWriteAttrs = workerSupplies.getBatchWriteAttrs();
        this.interval = persistenceInterval;
        this.bookCounterCaches = new ConcurrentHashMap<>();
        this.isEnabled = (interval != 0);
        LOGGER.info("Statistics worker status is {}", this.isEnabled);
    }

    private ScheduledExecutorService executorService;
    public void start() {
        if (!isEnabled) {
            LOGGER.info("Counter persistence service is disabled");
            return;
        }

        LOGGER.info("Starting executor for StatisticsWorker");
        executorService = Executors.newScheduledThreadPool(1, THREAD_FACTORY);
        executorService.scheduleAtFixedRate(this, 0, interval, TimeUnit.MILLISECONDS);
        LOGGER.info("StatisticsWorker executor started");
    }

    public void stop() {
        if (!isEnabled)
            return;

        if (executorService == null)
            throw new IllegalStateException("Can not stop statistics worker as it is not started");

        boolean interrupted = false;
        // ensure that cache is empty before executor service initiating shutdown
        if (bookCounterCachesNotEmpty()) {
            LOGGER.info("Waiting statistics cache depletion");
            while (bookCounterCachesNotEmpty())
                try {
                    Thread.sleep(100); // FIXME: find another way to wait the empty cache state
                } catch (InterruptedException e) {
                    LOGGER.error("Interrupted while waiting statistics cache depletion");
                    interrupted = true;
                }
        }

        // shut down executor service and wait for current job to complete
        LOGGER.info("Shutting down StatisticsWorker executor");
        executorService.shutdown();
        try {
            LOGGER.debug("Waiting StatisticsWorker jobs to complete");
            if (executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS)) {
                LOGGER.info("StatisticsWorker shutdown complete");
            } else {
                int neverCommencedTasks = executorService.shutdownNow().size();
                LOGGER.warn("StatisticsWorker executor service can't be stopped gracefully, " +
                        "{} tasks will never be commenced", neverCommencedTasks);
            }
        } catch (InterruptedException e) {
            LOGGER.error("Interrupted while waiting jobs to complete", e);
            interrupted = true;
        }

        // After executor service stops, we need to wait for futures to complete
        try {
            futures.awaitRemaining();
        } catch (InterruptedException e) {
            LOGGER.error("Interrupted while waiting tracked futures to complete");
            interrupted = true;
        }

        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private BookStatisticsRecordsCaches getBookStatisticsRecordsCaches(BookId bookId) {
        return bookCounterCaches.computeIfAbsent(bookId, k -> new BookStatisticsRecordsCaches(bookId));
    }


    private boolean bookCounterCachesNotEmpty() {
        return bookCounterCaches.values().stream().anyMatch(BookStatisticsRecordsCaches::notEmpty);
    }


    private void updateCounters(TimeFrameRecordCache<Counter> counters, Collection<? extends SerializedEntityMetadata> batchMetadata) {
        batchMetadata.forEach(meta -> {
            Counter counter = new Counter(1, meta.getSerializedEntitySize());
            for (FrameType t : FrameType.values()) {
                counters.getRecordSamples(t).update(meta.getTimestamp(), counter);
            }
        });
    }


    @Override
    public void updateEntityBatchStatistics(BookId bookId, BookStatisticsRecordsCaches.EntityKey entityKey, Collection<? extends SerializedEntityMetadata> batchMetadata) {

        if (!isEnabled)
            return;

        TimeFrameRecordCache<Counter> counters = getBookStatisticsRecordsCaches(bookId)
                .getEntityCounterCache()
                .get(entityKey);
        updateCounters(counters, batchMetadata);
    }


    @Override
    public void updateMessageBatchStatistics(BookId bookId, String page, String sessionAlias, String direction, Collection<SerializedMessageMetadata> batchMetadata) {

        if (!isEnabled)
            return;

        TimeFrameRecordCache<Counter> counters = getBookStatisticsRecordsCaches(bookId)
                .getMessageCounterCache()
                .get(new BookStatisticsRecordsCaches.MessageKey(page, sessionAlias, direction));
        updateCounters(counters, batchMetadata);
        BookStatisticsRecordsCaches.EntityKey key = new BookStatisticsRecordsCaches.EntityKey(page, EntityType.MESSAGE);
        // update entity statistics separately
        updateEntityBatchStatistics(bookId, key, batchMetadata);
    }

    @Override
    public void updateSessionStatistics(BookId bookId, String page, SessionRecordType recordType, String session, Collection<SerializedMessageMetadata> batchMetadata) {

        if (!isEnabled)
            return;

        TimeFrameRecordCache<SessionList> samples = getBookStatisticsRecordsCaches(bookId)
                .getSessionRecordCache()
                .get(new BookStatisticsRecordsCaches.SessionRecordKey(page, recordType));

        batchMetadata.forEach(meta -> {
            for (FrameType t : FrameType.values()) {
                samples.getRecordSamples(t).update(meta.getTimestamp(), new SessionList(session));
            }
        });
    }


    private void persistEntityCounters(BookId bookId,
                                       BookStatisticsRecordsCaches.EntityKey key,
                                       FrameType frameType,
                                       Collection<TimeFrameRecord<Counter>> records) throws InterruptedException {

        LOGGER.trace("Persisting {} entity counters for {}:{}:{}",
                records.size(),
                bookId,
                key,
                frameType);
        final EntityStatisticsOperator op = operators.getEntityStatisticsOperator();

        List<EntityStatisticsEntity> batch = new ArrayList<>();
        for (TimeFrameRecord<Counter> counter : records) {
            batch.add(new EntityStatisticsEntity(   bookId.getName(),
                                                    key.getPage(),
                                                    key.getEntityType().getValue(),
                                                    frameType.getValue(),
                                                    counter.getFrameStart(),
                                                    counter.getRecord().getEntityCount(),
                                                    counter.getRecord().getEntitySize()));
            if (batch.size() >= MAX_BATCH_STATEMENTS) {
                persistEntityCounters(op, bookId, key, frameType, batch);
                batch = new ArrayList<>();
            }
        }

        // persist any leftover batch
        if (!batch.isEmpty()) {
            persistEntityCounters(op, bookId, key, frameType, batch);
        }
    }


    private void persistEntityCounters(EntityStatisticsOperator op,
                                       BookId bookId,
                                       BookStatisticsRecordsCaches.EntityKey key,
                                       FrameType frameType,
                                       List<EntityStatisticsEntity> batch) throws InterruptedException {
        LOGGER.debug("Persisting batch of {} entity statistic records", batch.size());
        CompletableFuture<AsyncResultSet> future = op.update(batch, batchWriteAttrs);
        if(!futures.track(future)) {
            LOGGER.warn("Update entity statistic future isn't tracked");
        }
        future.whenComplete((result, e) -> {
            if (e != null) {
                LOGGER.error("Exception persisting message counters, rescheduling", e);
                TimeFrameRecordSamples<Counter> samples = getBookStatisticsRecordsCaches(bookId)
                        .getEntityCounterCache()
                        .get(key)
                        .getRecordSamples(frameType);
                batch.forEach(record -> samples.update(record.getFrameStart(), new Counter(record.getEntityCount(), record.getEntitySize())));
            }
        });
    }


    private void persistMessageCounters(BookId bookId,
                                        BookStatisticsRecordsCaches.MessageKey key,
                                        FrameType frameType,
                                        Collection<TimeFrameRecord<Counter>> records) throws InterruptedException {

        LOGGER.trace("Persisting {} message counters for {}:{}:{}:{}",
                records.size(),
                bookId,
                key.getSessionAlias(),
                key.getDirection(),
                frameType);
        final MessageStatisticsOperator op = operators.getMessageStatisticsOperator();

        List<MessageStatisticsEntity> batch = new ArrayList<>();
        for (TimeFrameRecord<Counter> counter : records) {
            batch.add(new MessageStatisticsEntity(  bookId.getName(),
                                                    key.getPage(),
                                                    key.getSessionAlias(),
                                                    key.getDirection(),
                                                    frameType.getValue(),
                                                    counter.getFrameStart(),
                                                    counter.getRecord().getEntityCount(),
                                                    counter.getRecord().getEntitySize()));
            if (batch.size() >= MAX_BATCH_STATEMENTS) {
                persistMessageCounters(op, bookId, key, frameType, batch);
                batch = new ArrayList<>();
            }
        }

        // persist any leftover batch
        if (!batch.isEmpty()) {
            persistMessageCounters(op, bookId, key, frameType, batch);
        }
    }


    private void persistMessageCounters(MessageStatisticsOperator op,
                                        BookId bookId,
                                        BookStatisticsRecordsCaches.MessageKey key,
                                        FrameType frameType,
                                        List<MessageStatisticsEntity> batch) throws InterruptedException {
        LOGGER.debug("Persisting batch of {} message statistic records", batch.size());
        CompletableFuture<AsyncResultSet> future = op.update(batch, batchWriteAttrs);
        if(!futures.track(future)) {
            LOGGER.warn("Update message statistic future isn't tracked");
        }
        future.whenComplete((result, e) -> {
            if (e != null) {
                LOGGER.error("Exception persisting message counters, rescheduling", e);
                TimeFrameRecordSamples<Counter> samples = getBookStatisticsRecordsCaches(bookId)
                        .getMessageCounterCache()
                        .get(key)
                        .getRecordSamples(frameType);
                batch.forEach(record -> samples.update(record.getFrameStart(), new Counter(record.getEntityCount(), record.getEntitySize())));
            }
        });
    }


    private void persistSessionStatistics(BookId bookId,
                                          BookStatisticsRecordsCaches.SessionRecordKey key,
                                          FrameType frameType,
                                          Collection<TimeFrameRecord<SessionList>> records) throws InterruptedException {

        LOGGER.trace("Persisting session statistics for {}:{}:{}:{} with {} records",
                bookId ,
                key.getPage() ,
                key.getRecordType() ,
                frameType ,
                records.size());

        final SessionStatisticsOperator op = operators.getSessionStatisticsOperator();
        final LimitedCache<SessionStatisticsEntity> cache = operators.getSessionStatisticsCache();

        List<SessionStatisticsEntity> batch = new ArrayList<>();

        for (TimeFrameRecord<SessionList> sessionsRecord: records) {
            for (String session : sessionsRecord.getRecord().getSessions()) {
                SessionStatisticsEntity entity = new SessionStatisticsEntity(   bookId.getName(),
                                                                                key.getPage(),
                                                                                key.getRecordType().getValue(),
                                                                                frameType.getValue(),
                                                                                sessionsRecord.getFrameStart(),
                                                                                session);
                if (!cache.contains(entity)) {
                    LOGGER.trace("Batching {}", entity);
                    batch.add(entity);
                } else {
                    LOGGER.trace("{} found in cache, ignoring", entity);
                }

                if (batch.size() >= MAX_BATCH_STATEMENTS) {
                    persistSessionStatistics(op, cache, bookId, key, frameType, batch);
                    batch = new ArrayList<>();
                }
            }
        }

        // persist any leftover batch
        if (!batch.isEmpty()) {
            persistSessionStatistics(op, cache, bookId, key, frameType, batch);
        }
    }


    private void persistSessionStatistics(SessionStatisticsOperator op,
                                          LimitedCache<SessionStatisticsEntity> cache,
                                          BookId bookId,
                                          BookStatisticsRecordsCaches.SessionRecordKey key,
                                          FrameType frameType,
                                          List<SessionStatisticsEntity> batch) throws InterruptedException {
        LOGGER.debug("Persisting batch of {} session statistic records", batch.size());
        CompletableFuture<AsyncResultSet> future = op.write(batch, batchWriteAttrs);
        if(!futures.track(future)) {
            LOGGER.warn("Update session statistic future isn't tracked");
        }
        future.whenComplete((result, e) -> {
            if (e != null) {
                LOGGER.error("Exception persisting session statistics, rescheduling", e);
                TimeFrameRecordSamples<SessionList> samples = getBookStatisticsRecordsCaches(bookId)
                        .getSessionRecordCache()
                        .get(key)
                        .getRecordSamples(frameType);
                batch.forEach(record -> samples.update(record.getFrameStart(), new SessionList(record.getSession())));
            } else {
                batch.forEach(cache::store);
            }
        });
    }


    private <K extends BookStatisticsRecordsCaches.RecordKey, V> void processRecords(
            BookId bookId,
            BookStatisticsRecordsCaches.RecordCache<K, V> recordCache,
            Persistor<K, V> persistor) throws InterruptedException {
        Collection<K> keys = recordCache.getKeys();
        for (K key : keys) {
            TimeFrameRecordCache<V> timeFrameRecords = recordCache.extract(key);
            if (timeFrameRecords != null)
                for (FrameType frameType : FrameType.values()) {
                    persistor.persist(bookId, key, frameType, timeFrameRecords.getRecordSamples(frameType).extractAll());
                }
        }
    }


    @Override
    public void run() {
        LOGGER.trace("executing StatisticsWorker job");

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
            LOGGER.error("Exception processing job", e);
        }

        LOGGER.trace("StatisticsWorker job complete");
    }

    @FunctionalInterface
    private interface Persistor<K extends BookStatisticsRecordsCaches.RecordKey, V> {
        void persist(BookId bookId, K key, FrameType frameType, Collection<TimeFrameRecord<V>> records) throws InterruptedException;
    }
}
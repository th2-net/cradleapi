/*
 * Copyright 2020-2023 Exactpro (Exactpro Systems Limited)
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
import com.exactpro.cradle.BookId;
import com.exactpro.cradle.EntityType;
import com.exactpro.cradle.FrameType;
import com.exactpro.cradle.SessionRecordType;
import com.exactpro.cradle.cassandra.counters.*;
import com.exactpro.cradle.cassandra.dao.CassandraOperators;
import com.exactpro.cradle.cassandra.dao.statistics.*;
import com.exactpro.cradle.cassandra.dao.statistics.scopes.ScopeStatisticsEntity;
import com.exactpro.cradle.cassandra.dao.statistics.scopes.ScopeStatisticsOperator;
import com.exactpro.cradle.cassandra.utils.LimitedCache;
import com.exactpro.cradle.counters.Counter;
import com.exactpro.cradle.serialization.SerializedEntityMetadata;
import com.exactpro.th2.taskutils.FutureTracker;
import com.sun.source.tree.Scope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.function.Consumer;
import java.util.function.Function;

public class StatisticsWorker implements Runnable, EntityStatisticsCollector, MessageStatisticsCollector, SessionStatisticsCollector, ScopeStatisticsCollector {

    private static final Logger logger = LoggerFactory.getLogger(StatisticsWorker.class);
    private static final int MAX_BATCH_STATEMENTS = 16384;

    private final FutureTracker<AsyncResultSet> futures;
    private final CassandraOperators operators;
    private final long interval;
    private final Map<BookId, BookStatisticsRecordsCaches> bookCounterCaches;
    private final Function<BatchStatementBuilder, BatchStatementBuilder> batchWriteAttrs;
    private final boolean isEnabled;

    public StatisticsWorker(WorkerSupplies workerSupplies, long persistenceInterval) {
        this.futures = new FutureTracker<>();
        this.operators = workerSupplies.getOperators();
        this.batchWriteAttrs = workerSupplies.getBatchWriteAttrs();
        this.interval = persistenceInterval;
        this.bookCounterCaches = new ConcurrentHashMap<>();
        this.isEnabled = (interval != 0);
    }

    private ScheduledExecutorService executorService;

    public void start() {
        if (!isEnabled) {
            logger.info("Counter persistence service is disabled");
            return;
        }

        logger.info("Starting executor for StatisticsWorker");
        executorService = Executors.newScheduledThreadPool(1);
        executorService.scheduleAtFixedRate(this, 0, interval, TimeUnit.MILLISECONDS);
        logger.info("StatisticsWorker executor started");
    }

    public void stop() {
        if (!isEnabled)
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

        // After executor service stops, we need to wait for futures to complete
        futures.awaitRemaining();
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

        if (!isEnabled)
            return;

        TimeFrameRecordCache<Counter> counters = getBookStatisticsRecordsCaches(bookId)
                .getEntityCounterCache()
                .get(entityKey);
        updateCounters(counters, batchMetadata);
    }


    @Override
    public void updateMessageBatchStatistics(BookId bookId, String page, String sessionAlias, String direction, Collection<SerializedEntityMetadata> batchMetadata) {

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
    public void updateSessionStatistics(BookId bookId, String page, SessionRecordType recordType, String session, Collection<SerializedEntityMetadata> batchMetadata) {

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

    @Override
    public void updateScopeStatistics(BookId bookId, String page, String scope, Collection<SerializedEntityMetadata> batchMetadata) {

        if (!isEnabled)
            return;

        TimeFrameRecordCache<ScopeList> samples = getBookStatisticsRecordsCaches(bookId)
                .getScopeRecordCache()
                .get(new BookStatisticsRecordsCaches.ScopeRecordKey(page));

        batchMetadata.forEach(meta -> {
            for (FrameType t : FrameType.values()) {
                samples.getRecordSamples(t).update(meta.getTimestamp(), new ScopeList(scope));
            }
        });
    }


    private void persistEntityCounters(BookId bookId, BookStatisticsRecordsCaches.EntityKey key, FrameType frameType, Collection<TimeFrameRecord<Counter>> records) {

        logger.trace("Persisting {} entity counters for {}:{}:{}", records.size(), bookId, key, frameType);
        final EntityStatisticsOperator op = operators.getEntityStatisticsOperator();

        final Consumer<List<EntityStatisticsEntity>> persistor = (List<EntityStatisticsEntity> batch) -> {

            logger.debug("Persisting batch of {} entity statistic records", batch.size());
            CompletableFuture<AsyncResultSet> future = op.update(batch, batchWriteAttrs);
            futures.track(future);
            future.whenComplete((result, e) -> {
                if (e != null) {
                    logger.error("Exception persisting message counters, rescheduling", e);
                    TimeFrameRecordSamples<Counter> samples = getBookStatisticsRecordsCaches(bookId)
                            .getEntityCounterCache()
                            .get(key)
                            .getRecordSamples(frameType);
                    batch.forEach(record -> samples.update(record.getFrameStart(), new Counter(record.getEntityCount(), record.getEntitySize())));
                }
            });
        };

        List<EntityStatisticsEntity> batch = new ArrayList<>();
        for (TimeFrameRecord<Counter> counter : records) {
            batch.add(new EntityStatisticsEntity(bookId.getName(),
                    key.getPage(),
                    key.getEntityType().getValue(),
                    frameType.getValue(),
                    counter.getFrameStart(),
                    counter.getRecord().getEntityCount(),
                    counter.getRecord().getEntitySize()));
            if (batch.size() >= MAX_BATCH_STATEMENTS) {
                persistor.accept(batch);
                batch = new ArrayList<>();
            }
        }

        // persist any leftover batch
        if (batch.size() > 0) {
            persistor.accept(batch);
        }
    }


    private void persistMessageCounters(BookId bookId, BookStatisticsRecordsCaches.MessageKey key, FrameType frameType, Collection<TimeFrameRecord<Counter>> records) {

        logger.trace("Persisting {} message counters for {}:{}:{}:{}", records.size(), bookId, key.getSessionAlias(), key.getDirection(), frameType);
        final MessageStatisticsOperator op = operators.getMessageStatisticsOperator();

        final Consumer<List<MessageStatisticsEntity>> persistor = (List<MessageStatisticsEntity> batch) -> {

            logger.debug("Persisting batch of {} message statistic records", batch.size());
            CompletableFuture<AsyncResultSet> future = op.update(batch, batchWriteAttrs);
            futures.track(future);
            future.whenComplete((result, e) -> {
                if (e != null) {
                    logger.error("Exception persisting message counters, rescheduling", e);
                    TimeFrameRecordSamples<Counter> samples = getBookStatisticsRecordsCaches(bookId)
                            .getMessageCounterCache()
                            .get(key)
                            .getRecordSamples(frameType);
                    batch.forEach(record -> samples.update(record.getFrameStart(), new Counter(record.getEntityCount(), record.getEntitySize())));
                }
            });
        };

        List<MessageStatisticsEntity> batch = new ArrayList<>();
        for (TimeFrameRecord<Counter> counter : records) {
            batch.add(new MessageStatisticsEntity(bookId.getName(),
                    key.getPage(),
                    key.getSessionAlias(),
                    key.getDirection(),
                    frameType.getValue(),
                    counter.getFrameStart(),
                    counter.getRecord().getEntityCount(),
                    counter.getRecord().getEntitySize()));
            if (batch.size() >= MAX_BATCH_STATEMENTS) {
                persistor.accept(batch);
                batch = new ArrayList<>();
            }
        }

        // persist any leftover batch
        if (batch.size() > 0) {
            persistor.accept(batch);
        }
    }


    private void persistSessionStatistics(BookId bookId, BookStatisticsRecordsCaches.SessionRecordKey key, FrameType frameType, Collection<TimeFrameRecord<SessionList>> records) {

        logger.trace("Persisting session statistics for {}:{}:{}:{} with {} records"
                , bookId
                , key.getPage()
                , key.getRecordType()
                , frameType
                , records.size());

        final SessionStatisticsOperator op = operators.getSessionStatisticsOperator();
        final LimitedCache<SessionStatisticsEntity> cache = operators.getSessionStatisticsCache();

        final Consumer<List<SessionStatisticsEntity>> persistor = (List<SessionStatisticsEntity> batch) -> {

            logger.debug("Persisting batch of {} session statistic records", batch.size());
            CompletableFuture<AsyncResultSet> future = op.write(batch, batchWriteAttrs);
            futures.track(future);
            future.whenComplete((result, e) -> {
                if (e != null) {
                    logger.error("Exception persisting session statistics, rescheduling", e);
                    TimeFrameRecordSamples<SessionList> samples = getBookStatisticsRecordsCaches(bookId)
                            .getSessionRecordCache()
                            .get(key)
                            .getRecordSamples(frameType);
                    batch.forEach(record -> samples.update(record.getFrameStart(), new SessionList(record.getSession())));
                } else {
                    batch.forEach(cache::store);
                }
            });
        };

        List<SessionStatisticsEntity> batch = new ArrayList<>();

        for (TimeFrameRecord<SessionList> sessionsRecord : records) {
            for (String session : sessionsRecord.getRecord().getSessions()) {
                SessionStatisticsEntity entity = new SessionStatisticsEntity(bookId.getName(),
                        key.getPage(),
                        key.getRecordType().getValue(),
                        frameType.getValue(),
                        sessionsRecord.getFrameStart(),
                        session);
                if (!cache.contains(entity)) {
                    logger.trace("Batching {}", entity);
                    batch.add(entity);
                } else {
                    logger.trace("{} found in cache, ignoring", entity);
                }

                if (batch.size() >= MAX_BATCH_STATEMENTS) {
                    persistor.accept(batch);
                    batch = new ArrayList<>();
                }
            }
        }

        // persist any leftover batch
        if (batch.size() > 0) {
            persistor.accept(batch);
        }
    }

    private void persistScopeStatistics(BookId bookId, BookStatisticsRecordsCaches.ScopeRecordKey key, FrameType frameType, Collection<TimeFrameRecord<ScopeList>> records) {

        logger.trace("Persisting scope statistics for {}:{} with {} records"
                , bookId
                , frameType
                , records.size());

        final ScopeStatisticsOperator op = operators.getScopeStatisticsOperator();
        final LimitedCache<ScopeStatisticsEntity> cache = operators.getScopeStatisticsCache();

        final Consumer<List<ScopeStatisticsEntity>> persistor = (List<ScopeStatisticsEntity> batch) -> {

            logger.debug("Persisting batch of {} scope statistic records", batch.size());
            CompletableFuture<AsyncResultSet> future = op.write(batch, batchWriteAttrs);
            futures.track(future);
            future.whenComplete((result, e) -> {
                if (e != null) {
                    logger.error("Exception persisting scope statistics, rescheduling", e);
                    TimeFrameRecordSamples<ScopeList> samples = getBookStatisticsRecordsCaches(bookId)
                            .getScopeRecordCache()
                            .get(key)
                            .getRecordSamples(frameType);
                    batch.forEach(record -> samples.update(record.getFrameStart(), new ScopeList(record.getScope())));
                } else {
                    batch.forEach(cache::store);
                }
            });
        };

        List<ScopeStatisticsEntity> batch = new ArrayList<>();

        for (TimeFrameRecord<ScopeList> scopesRecord : records) {
            for (String scope : scopesRecord.getRecord().getScopes()) {
                ScopeStatisticsEntity entity = new ScopeStatisticsEntity(bookId.getName(),
                        key.getPage(),
                        frameType.getValue(),
                        scopesRecord.getFrameStart(),
                        scope);
                if (!cache.contains(entity)) {
                    logger.trace("Batching {}", entity);
                    batch.add(entity);
                } else {
                    logger.trace("{} found in cache, ignoring", entity);
                }

                if (batch.size() >= MAX_BATCH_STATEMENTS) {
                    persistor.accept(batch);
                    batch = new ArrayList<>();
                }
            }
        }

        // persist any leftover batch
        if (batch.size() > 0) {
            persistor.accept(batch);
        }
    }


    private <K extends BookStatisticsRecordsCaches.RecordKey, V> void processRecords(
            BookId bookId,
            BookStatisticsRecordsCaches.RecordCache<K, V> recordCache,
            Persistor<K, V> persistor) {
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
        logger.trace("executing StatisticsWorker job");

        try {
            for (BookId bookId : bookCounterCaches.keySet()) {

                BookStatisticsRecordsCaches bookStatisticsRecordsCaches = getBookStatisticsRecordsCaches(bookId);

                // persist all cached entity counters
                processRecords(bookId, bookStatisticsRecordsCaches.getEntityCounterCache(), this::persistEntityCounters);

                // persist all cached message counters
                processRecords(bookId, bookStatisticsRecordsCaches.getMessageCounterCache(), this::persistMessageCounters);

                // persist all cached session statistics
                processRecords(bookId, bookStatisticsRecordsCaches.getSessionRecordCache(), this::persistSessionStatistics);

                // persist all cached scope statistics
                processRecords(bookId, bookStatisticsRecordsCaches.getScopeRecordCache(), this::persistScopeStatistics);
            }
        } catch (Exception e) {
            logger.error("Exception processing job", e);
        }

        logger.trace("StatisticsWorker job complete");
    }


    private interface Persistor<K extends BookStatisticsRecordsCaches.RecordKey, V> {
        void persist(BookId bookId, K key, FrameType frameType, Collection<TimeFrameRecord<V>> records);
    }
}
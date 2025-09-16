/*
 * Copyright 2021-2025 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.cradle.BookId;
import com.exactpro.cradle.BookInfo;
import com.exactpro.cradle.EntityType;
import com.exactpro.cradle.PageId;
import com.exactpro.cradle.cassandra.EventBatchDurationWorker;
import com.exactpro.cradle.cassandra.counters.BookStatisticsRecordsCaches;
import com.exactpro.cradle.cassandra.counters.EntityStatisticsCollector;
import com.exactpro.cradle.cassandra.dao.CassandraOperators;
import com.exactpro.cradle.cassandra.dao.cache.CachedPageScope;
import com.exactpro.cradle.cassandra.dao.cache.CachedScope;
import com.exactpro.cradle.cassandra.dao.testevents.PageScopeEntity;
import com.exactpro.cradle.cassandra.dao.testevents.ScopeEntity;
import com.exactpro.cradle.cassandra.dao.testevents.TestEventEntity;
import com.exactpro.cradle.cassandra.dao.testevents.TestEventEntityUtils;
import com.exactpro.cradle.cassandra.dao.testevents.TestEventIteratorProvider;
import com.exactpro.cradle.cassandra.dao.testevents.TestEventOperator;
import com.exactpro.cradle.cassandra.dao.testevents.converters.TestEventEntityConverter;
import com.exactpro.cradle.cassandra.resultset.CassandraCradleResultSet;
import com.exactpro.cradle.resultset.CradleResultSet;
import com.exactpro.cradle.serialization.SerializedEntityMetadata;
import com.exactpro.cradle.testevents.StoredTestEvent;
import com.exactpro.cradle.testevents.StoredTestEventId;
import com.exactpro.cradle.testevents.TestEventFilter;
import com.exactpro.cradle.testevents.TestEventToStore;
import com.exactpro.cradle.utils.CompressException;
import com.exactpro.cradle.utils.CradleIdException;
import com.exactpro.cradle.utils.CradleStorageException;
import com.exactpro.cradle.utils.TimeUtils;
import io.prometheus.client.Counter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static java.util.Objects.requireNonNull;

public class EventsWorker extends Worker {
    private static final Logger logger = LoggerFactory.getLogger(EventsWorker.class);

    private static final MetricHolder<StreamLabel> EVENTS_READ_METRIC = new MetricHolder<>(
            Counter.build()
                    .name("cradle_test_events_read_total")
                    .help("Fetched test events")
                    .labelNames(StreamLabel.LABEL_NAMES)
                    .register()
    );
    private static final MetricHolder<StreamLabel> EVENT_BATCHES_READ_METRIC = new MetricHolder<>(
            Counter.build()
                    .name("cradle_test_event_batches_read_total")
                    .help("Fetched test event batches")
                    .labelNames(StreamLabel.LABEL_NAMES)
                    .register()
    );
    private static final MetricHolder<StreamLabel> EVENTS_STORE_METRIC = new MetricHolder<>(
            Counter.build()
                    .name("cradle_test_events_stored_total")
                    .help("Stored test events")
                    .labelNames(StreamLabel.LABEL_NAMES)
                    .register()
    );
    private static final MetricHolder<StreamLabel> EVENT_BATCHES_STORE_METRIC = new MetricHolder<>(
            Counter.build()
                    .name("cradle_test_event_batches_stored_total")
                    .help("Stored test event batches")
                    .labelNames(StreamLabel.LABEL_NAMES)
                    .register()
    );
    private static final MetricHolder<StreamLabel> EVENTS_STORE_UNCOMPRESSED_BYTES = new MetricHolder<>(
            Counter.build()
                    .name("cradle_test_events_stored_uncompressed_bytes_total")
                    .help("Stored uncompressed event bytes")
                    .labelNames(StreamLabel.LABEL_NAMES)
                    .register()
    );
    private static final MetricHolder<StreamLabel> EVENTS_STORE_COMPRESSED_BYTES = new MetricHolder<>(
            Counter.build()
                    .name("cradle_test_events_stored_compressed_bytes_total")
                    .help("Stored compressed event bytes")
                    .labelNames(StreamLabel.LABEL_NAMES)
                    .register()
    );

    private final EntityStatisticsCollector entityStatisticsCollector;
    private final EventBatchDurationWorker durationWorker;

    public EventsWorker(WorkerSupplies workerSupplies, EntityStatisticsCollector entityStatisticsCollector, EventBatchDurationWorker durationWorker) {
        super(workerSupplies);
        this.entityStatisticsCollector = entityStatisticsCollector;
        this.durationWorker = durationWorker;
    }

    public static StoredTestEvent mapTestEventEntity(PageId pageId, TestEventEntity entity) {
        try {
            StoredTestEvent testEvent = TestEventEntityUtils.toStoredTestEvent(entity, pageId);
            updateEventReadMetrics(testEvent);
            return testEvent;
        } catch (CradleStorageException | CradleIdException | IOException | CompressException e) {
            throw new CompletionException("Error while converting test event entity into Cradle test event", e);
        }
    }

    private static void updateEventReadMetrics(StoredTestEvent testEvent) {
        StreamLabel key = new StreamLabel(testEvent.getId().getBookId().getName(), testEvent.getScope());
        if (testEvent.isSingle()) {
            EVENTS_READ_METRIC.inc(key);
        } else if (testEvent.isBatch()) {
            EVENTS_READ_METRIC.inc(key, testEvent.asBatch().getTestEventsCount());
        }
        EVENT_BATCHES_READ_METRIC.inc(key);
    }

    private static void updateEventWriteMetrics(TestEventEntity entity, BookId bookId) {
        StreamLabel key = new StreamLabel(bookId.getName(), entity.getScope());
        EVENTS_STORE_METRIC.inc(key, entity.isEventBatch() ? entity.getEventCount() : 1);
        EVENT_BATCHES_STORE_METRIC.inc(key);
        EVENTS_STORE_UNCOMPRESSED_BYTES.inc(key, entity.getUncompressedContentSize());
        EVENTS_STORE_COMPRESSED_BYTES.inc(key, entity.getContentSize());
    }

    public CompletableFuture<Void> storeEvent(TestEventToStore event, PageId pageId) {
        TestEventOperator op = getOperators().getTestEventOperator();
        BookStatisticsRecordsCaches.EntityKey key = new BookStatisticsRecordsCaches.EntityKey(pageId.getName(), EntityType.EVENT);

        return CompletableFuture.supplyAsync(() -> {
            try {
                return TestEventEntityUtils.toSerializedEntity(event, pageId, settings.getCompressionType(), settings.getMaxUncompressedMessageBatchSize());
            } catch (Exception e) {
                throw new CompletionException(e);
            }
        }, composingService).thenCompose(serializedEntity -> {
            TestEventEntity entity = serializedEntity.getEntity();
            List<SerializedEntityMetadata> meta = serializedEntity.getSerializedEntityData().getSerializedEntityMetadata();

            return op.write(entity, writeAttrs)
                    .thenAcceptAsync(result -> {
                        try {
                            Instant firstTimestamp = meta.get(0).getTimestamp();
                            Instant lastStartTimestamp = firstTimestamp;
                            for (SerializedEntityMetadata el : meta) {
                                if (el.getTimestamp() != null) {
                                    if (firstTimestamp.isAfter(el.getTimestamp())) {
                                        firstTimestamp = el.getTimestamp();
                                    }
                                    if (lastStartTimestamp.isBefore(el.getTimestamp())) {
                                        lastStartTimestamp = el.getTimestamp();
                                    }
                                }
                            }
                            durationWorker.updateMaxDuration(pageId, entity.getScope(),
                                    Duration.between(firstTimestamp, lastStartTimestamp).toMillis(),
                                    writeAttrs);
                        } catch (CradleStorageException e) {
                            logger.error("Exception while updating max duration {}", e.getMessage());
                        }

                        entityStatisticsCollector.updateEntityBatchStatistics(pageId.getBookId(), key, meta);
                        updateEventWriteMetrics(entity, pageId.getBookId());
                    }, composingService);
        });
    }

    public CompletableFuture<ScopeEntity> storeScope(TestEventToStore event) {
        String bookName = event.getBookId().getName();
        CassandraOperators operators = getOperators();
        if (!operators.getScopesCache().store(new CachedScope(bookName, event.getScope()))) {
            logger.debug("Skipped writing scope of event '{}'", event.getId());
            return CompletableFuture.completedFuture(null);
        }

        logger.debug("Writing scope of event '{}'", event.getId());
        return operators.getScopeOperator()
                .write(new ScopeEntity(bookName, event.getScope()), writeAttrs);
    }

    public CompletableFuture<PageScopeEntity> storePageScope(TestEventToStore event, PageId pageId) {
        CassandraOperators operators = getOperators();
        if (!operators.getPageScopesCache().store(new CachedPageScope(pageId.toString(), event.getScope()))) {
            logger.debug("Skipped writing scope partition of event '{}'", event.getId());
            return CompletableFuture.completedFuture(null);
        }

        logger.debug("Writing scope partition of event '{}'", event.getId());
        return operators.getPageScopesOperator()
                .write(new PageScopeEntity(pageId.getBookId().getName(), pageId.getName(), event.getScope()), writeAttrs);
    }

    public CompletableFuture<StoredTestEvent> getTestEvent(StoredTestEventId id, PageId pageId) {
        LocalDateTime ldt = TimeUtils.toLocalTimestamp(id.getStartTimestamp());
        CassandraOperators operators = getOperators();
        TestEventEntityConverter converter = operators.getTestEventEntityConverter();
        return selectQueryExecutor.executeSingleRowResultQuery(
                        () -> operators.getTestEventOperator().get(pageId.getBookId().getName(), pageId.getName(), id.getScope(),
                                ldt.toLocalDate(), ldt.toLocalTime(), id.getId(), readAttrs), converter::getEntity,
                        String.format("get test event by id '%s'", id))
                .thenApplyAsync(entity -> {
                    if (entity == null)
                        return null;

                    try {
                        StoredTestEvent testEvent = TestEventEntityUtils.toStoredTestEvent(entity, pageId);
                        updateEventReadMetrics(testEvent);
                        return  testEvent;
                    } catch (Exception e) {
                        throw new CompletionException("Error while converting data of event " + id + " into test event", e);
                    }
                }, composingService);
    }

    public CompletableFuture<CradleResultSet<StoredTestEvent>> getTestEvents(TestEventFilter filter, BookInfo book) {
        Instant startTimestamp;

        if (filter.getStartTimestampFrom() == null) {
            if (filter.getPageId() == null) {
                startTimestamp = book.getFirstPage().getStarted();
            } else {
                startTimestamp = book.getPage(filter.getPageId()).getStarted();
            }
        } else {
            startTimestamp = filter.getStartTimestampFrom().getValue();
        }

        TestEventIteratorProvider provider = new TestEventIteratorProvider("get test events filtered by " + filter,
                filter, getOperators(), book, composingService, selectQueryExecutor,
                durationWorker,
                composeReadAttrs(filter.getFetchParameters()),
                startTimestamp);
        return provider.nextIterator()
                .thenApply(r -> new CassandraCradleResultSet<>(r, provider));
    }

    public CompletableFuture<Void> updateStatus(StoredTestEvent event, boolean success) {
        StoredTestEventId id = event.getId();
        LocalDateTime ldt = TimeUtils.toLocalTimestamp(event.getStartTimestamp());
        PageId pageId = event.getPageId();
        return getOperators().getTestEventOperator().updateStatus(pageId.getBookId().getName(), pageId.getName(), id.getScope(),
                ldt.toLocalDate(), ldt.toLocalTime(), id.getId(), success, writeAttrs);
    }

    private static class StreamLabel implements MetricHolder.LabelHolder {
        private static final String[] LABEL_NAMES = new String[]{BOOK_ID, SCOPE};
        private final String bookId;
        private final String scope;
        private final int hash;

        private StreamLabel(
                @Nonnull String bookId,
                @Nonnull String scope
        ) {
            this.bookId = requireNonNull(bookId, "'bookId' can't be null");
            this.scope = requireNonNull(scope, "'scope' can't be null");
            this.hash = Objects.hash(bookId, scope);
        }

        @Override
        public String[] getLabels() {
            return new String[]{bookId, scope};
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            StreamLabel that = (StreamLabel) o;
            return Objects.equals(bookId, that.bookId)
                    && Objects.equals(scope, that.scope);
        }

        @Override
        public int hashCode() {
            return hash;
        }
    }
}

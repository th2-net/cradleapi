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
package com.exactpro.cradle.cassandra.dao.testevents;

import com.exactpro.cradle.BookId;
import com.exactpro.cradle.PageId;
import com.exactpro.cradle.cassandra.dao.SerializedEntity;
import com.exactpro.cradle.messages.StoredMessageId;
import com.exactpro.cradle.serialization.SerializedEntityData;
import com.exactpro.cradle.serialization.SerializedEntityMetadata;
import com.exactpro.cradle.testevents.BatchedStoredTestEvent;
import com.exactpro.cradle.testevents.StoredTestEvent;
import com.exactpro.cradle.testevents.StoredTestEventBatch;
import com.exactpro.cradle.testevents.StoredTestEventId;
import com.exactpro.cradle.testevents.StoredTestEventSingle;
import com.exactpro.cradle.testevents.TestEventToStore;
import com.exactpro.cradle.utils.CompressException;
import com.exactpro.cradle.utils.CompressionType;
import com.exactpro.cradle.utils.CradleIdException;
import com.exactpro.cradle.utils.CradleStorageException;
import com.exactpro.cradle.utils.TestEventUtils;
import com.exactpro.cradle.utils.TimeUtils;
import io.prometheus.client.Counter;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionException;

public class TestEventEntityUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestEventEntityUtils.class);
    private static final String LABEL_TYPE = "type";
    private static final String LABEL_BATCH = "batch";
    private static final String LABEL_BATCHED_EVENT = "batched-event";
    private static final String LABEL_SINGLE_EVENT = "single-event";


    private static final Counter RESTORE_DURATION = Counter.build()
            .name("cradle_test_event_restore_duration_seconds")
            .help("Time spent restoring batch / batched-event / single-event data from optionally compressed content")
            .labelNames(LABEL_TYPE)
            .register();
    private static final Counter DESERIALISATION_DURATION = Counter.build()
            .name("cradle_test_event_deserialisation_duration_seconds")
            .help("Time spent deserializing batch / batched-event / single-event")
            .labelNames(LABEL_TYPE)
            .register();
    private static final Counter CONTENT_SIZE = Counter.build()
            .name("cradle_test_event_deserialisation_content_bytes_total")
            .help("Total size of content processed during event deserialization")
            .labelNames(LABEL_TYPE)
            .register();
    private static final Counter ITEMS_TOTAL = Counter.build()
            .name("cradle_test_event_deserialized_total")
            .help("Number of deserialized batch / batched-event / single-event")
            .labelNames(LABEL_TYPE)
            .register();

    private static final Metric batchMetric = new Metric(
            RESTORE_DURATION, DESERIALISATION_DURATION, CONTENT_SIZE, ITEMS_TOTAL, LABEL_BATCH);
    private static final Metric batchedEventMetric = new Metric(
            RESTORE_DURATION, DESERIALISATION_DURATION, CONTENT_SIZE, ITEMS_TOTAL, LABEL_BATCHED_EVENT);
    private static final Metric singleEventMetric = new Metric(
            RESTORE_DURATION, DESERIALISATION_DURATION, CONTENT_SIZE, ITEMS_TOTAL, LABEL_SINGLE_EVENT);

    public static StoredTestEvent toStoredTestEvent(TestEventEntity testEventEntity, PageId pageId) {
        try {
            StoredTestEventId eventId = createId(testEventEntity, pageId.getBookId());
            LOGGER.trace("Creating test event '{}' from entity", eventId);

            long t0 = System.nanoTime();
            ByteBuffer content = restoreContent(testEventEntity, eventId);
            int contentSize = content == null ? 0 : content.remaining();
            long t1 = System.nanoTime();
            double restoreSec = (t1 - t0) / 1_000_000_000.0;
            if (testEventEntity.isEventBatch()) {
                StoredTestEventBatch batch = toStoredTestEventBatch(testEventEntity, pageId, eventId, content);
                double deserializationSec = (System.nanoTime() - t1) / 1_000_000_000.0;
                batchMetric.inc(restoreSec, deserializationSec, contentSize, 1);
                batchedEventMetric.inc(restoreSec, deserializationSec, contentSize, batch.getTestEventsCount());
                return batch;
            } else {
                StoredTestEventSingle event = toStoredTestEventSingle(testEventEntity, pageId, eventId, content);
                singleEventMetric.inc(restoreSec, (System.nanoTime() - t1) / 1_000_000_000.0, contentSize, 1);
                return event;
            }
        } catch (IOException | CradleStorageException | CradleIdException | CompressException e) {
            throw new CompletionException("Error while converting test event entity into Cradle test event", e);
        }
    }

    private static StoredTestEventId createId(TestEventEntity testEventEntity, BookId bookId)
    {
        return new StoredTestEventId(bookId, testEventEntity.getScope(), TestEventEntityUtils.getStartTimestamp(testEventEntity), testEventEntity.getId());
    }

    private static StoredTestEventId createParentId(TestEventEntity testEventEntity) throws CradleIdException
    {
        return StringUtils.isEmpty(testEventEntity.getParentId()) ? null : StoredTestEventId.fromString(testEventEntity.getParentId());
    }

    private static ByteBuffer restoreContent(TestEventEntity testEventEntity, StoredTestEventId eventId) throws CompressException {
        ByteBuffer content = testEventEntity.getContent();
        if (content == null)
            return null;

        ByteBuffer result = content;
        if (testEventEntity.isCompressed()) {
            LOGGER.trace("Decompressing content of test event '{}'", eventId);
            result = ByteBuffer.allocate(testEventEntity.getUncompressedContentSize());
            CompressionType.decompressData(content, result);
        }
        return result;
    }

    private static Set<StoredMessageId> restoreMessages(TestEventEntity testEventEntity, BookId bookId)
            throws IOException {
        ByteBuffer messages = testEventEntity.getMessages();
        if (messages == null)
            return null;

        return TestEventUtils.deserializeLinkedMessageIds(messages, bookId);
    }

    private static Map<StoredTestEventId, Set<StoredMessageId>> restoreBatchMessages(
            TestEventEntity testEventEntity, BookId bookId) throws IOException {
        ByteBuffer messages = testEventEntity.getMessages();
        if (messages == null) return null;

        return TestEventUtils.deserializeBatchLinkedMessageIds(messages, bookId);
    }

    private static StoredTestEventSingle toStoredTestEventSingle(TestEventEntity testEventEntity, PageId pageId, StoredTestEventId eventId, ByteBuffer content)
            throws IOException, CradleIdException
    {
        Set<StoredMessageId> messages = restoreMessages(testEventEntity, pageId.getBookId());
        return new StoredTestEventSingle(eventId, testEventEntity.getName(), testEventEntity.getType(), createParentId(testEventEntity),
                TestEventEntityUtils.getEndTimestamp(testEventEntity), testEventEntity.isSuccess(), content, messages, pageId, null, testEventEntity.getRecDate());
    }

    private static StoredTestEventBatch toStoredTestEventBatch(TestEventEntity testEventEntity, PageId pageId,
                                                               StoredTestEventId eventId, ByteBuffer content)
            throws IOException, CradleStorageException, CradleIdException {
        Collection<BatchedStoredTestEvent> children = TestEventUtils.deserializeTestEvents(content, eventId);
        Map<StoredTestEventId, Set<StoredMessageId>> messages = restoreBatchMessages(testEventEntity, pageId.getBookId());
        return new StoredTestEventBatch(eventId, testEventEntity.getName(), testEventEntity.getType(), createParentId(testEventEntity),
                children, messages, pageId, null, testEventEntity.getRecDate());
    }

    public static Instant getEndTimestamp(TestEventEntity entity) {
        return TimeUtils.toInstant(entity.getEndDate(), entity.getEndTime());
    }

    public static Instant getStartTimestamp(TestEventEntity entity) {
        return TimeUtils.toInstant(entity.getStartDate(), entity.getStartTime());
    }

    public static SerializedEntity<SerializedEntityMetadata, TestEventEntity> toSerializedEntity(TestEventToStore event,
                                                                                                 PageId pageId,
                                                                                                 CompressionType compressionType,
                                                                                                 int maxUncompressedSize) throws IOException, CompressException {
        TestEventEntity.TestEventEntityBuilder builder = TestEventEntity.builder();

        LOGGER.debug("Creating entity from test event '{}'", event.getId());

        SerializedEntityData<SerializedEntityMetadata> serializedEntityData = TestEventUtils.getTestEventContent(event);
        byte[] content = serializedEntityData.getSerializedData();
        boolean compressed = false;

        if (content == null) {
            builder.setUncompressedContentSize(0);
            builder.setContentSize(0);
        } else {
            builder.setUncompressedContentSize(content.length);
            if (content.length > maxUncompressedSize) {
                LOGGER.trace("Compressing content of test event '{}'", event.getId());
                content = compressionType.compress(content);
                compressed = true;
            }
            builder.setContentSize(content.length);
        }

        byte[] messages = TestEventUtils.serializeLinkedMessageIds(event);

        StoredTestEventId parentId = event.getParentId();
        LocalDateTime start = TimeUtils.toLocalTimestamp(event.getStartTimestamp());

        builder.setBook(pageId.getBookId().getName());
        builder.setPage(pageId.getName());
        builder.setScope(event.getScope());
        builder.setStartTimestamp(start);
        builder.setId(event.getId().getId());

        builder.setSuccess(event.isSuccess());
        builder.setRoot(parentId == null);
        builder.setEventBatch(event.isBatch());
        builder.setName(event.getName());
        builder.setType(event.getType());
        builder.setParentId(parentId != null ? parentId.toString() : "");  //Empty string for absent parentId allows using index to get root events
        if (event.isBatch())
            builder.setEventCount(event.asBatch().getTestEventsCount());
        builder.setEndTimestamp(event.getEndTimestamp());

        if (messages != null)
            builder.setMessages(ByteBuffer.wrap(messages));

        builder.setCompressed(compressed);
        //TODO: this.setLabels(event.getLabels());
        if (content != null)
            builder.setContent(ByteBuffer.wrap(content));

        return new SerializedEntity<>(serializedEntityData, builder.build());
    }

    private static class Metric {
        private final Counter.Child restore;
        private final Counter.Child deserialization;
        private final Counter.Child content;
        private final Counter.Child count;

        private Metric(Counter restore, Counter deserialization, Counter content, Counter count, String type) {
            this.restore = restore.labels(type);
            this.deserialization = deserialization.labels(type);
            this.content = content.labels(type);
            this.count = count.labels(type);
        }

        private void inc(double restoreSec, double deserializationSec, int contentSize,  int count) {
            this.restore.inc(restoreSec);
            this.deserialization.inc(deserializationSec);
            this.content.inc(contentSize);
            this.count.inc(count);
        }
    }
}

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

package com.exactpro.cradle.utils;

import com.exactpro.cradle.BookId;
import com.exactpro.cradle.BookInfo;
import com.exactpro.cradle.PageInfo;
import com.exactpro.cradle.messages.StoredMessageId;
import com.exactpro.cradle.serialization.EventBatchCommonParams;
import com.exactpro.cradle.serialization.EventBatchDeserializer;
import com.exactpro.cradle.serialization.EventBatchSerializer;
import com.exactpro.cradle.serialization.EventMessageIdDeserializer;
import com.exactpro.cradle.serialization.EventMessageIdSerializer;
import com.exactpro.cradle.serialization.SerializedEntityData;
import com.exactpro.cradle.serialization.SerializedEntityMetadata;
import com.exactpro.cradle.testevents.BatchedStoredTestEvent;
import com.exactpro.cradle.testevents.StoredTestEventId;
import com.exactpro.cradle.testevents.TestEvent;
import com.exactpro.cradle.testevents.TestEventBatch;
import com.exactpro.cradle.testevents.TestEventSingle;
import com.exactpro.cradle.testevents.TestEventSingleToStore;
import com.exactpro.cradle.testevents.TestEventToStore;
import com.exactpro.cradle.testevents.lw.LwBatchedStoredTestEvent;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

public class TestEventUtils {
    private static final Logger logger = LoggerFactory.getLogger(TestEventUtils.class);

    private static final EventBatchDeserializer deserializer = new EventBatchDeserializer();
    private static final EventBatchSerializer serializer = new EventBatchSerializer();

    /**
     * Validates event with checks requiring bookInfo as well as necessary fields
     *
     * @param event    event
     * @param bookInfo bookInfo
     * @throws CradleStorageException if validation failed
     */
    public static void validateTestEvent(TestEvent event, BookInfo bookInfo, long storeActionRejectionThreshold) throws CradleStorageException {
        if (bookInfo != null && event.getParentId() != null) {
            PageInfo pageInfo = bookInfo.findPage(event.getParentId().getStartTimestamp());
            if (pageInfo == null) {
                throw new CradleStorageException(
                        String.format("Test event's parent event's startTimestamp is %s, could not find corresponding page in book %s",
                                event.getParentId().getStartTimestamp(),
                                bookInfo.getId()));
            }
        }

        validateTestEvent(event, storeActionRejectionThreshold);
    }

    /**
     * Checks that test event has all necessary fields set
     *
     * @param event to validate
     * @throws CradleStorageException if validation failed
     */
    public static void validateTestEvent(TestEvent event, long storeActionRejectionThreshold) throws CradleStorageException {
        if (event.getId() == null)
            throw new CradleStorageException("Test event ID cannot be null");

        if (event.getId().equals(event.getParentId()))
            throw new CradleStorageException("Test event cannot reference itself");

        if (event instanceof TestEventSingle && StringUtils.isEmpty(event.getName()))
            throw new CradleStorageException("Single test event must have a name");
        if (event instanceof TestEventBatch && event.getParentId() == null)
            throw new CradleStorageException("Batch must have a parent");

        if (event.getBookId() == null || StringUtils.isEmpty(event.getBookId().toString()))
            throw new CradleStorageException("Test event must have a book");
        if (StringUtils.isEmpty(event.getScope()))
            throw new CradleStorageException("Test event must have a scope");
        if (event.getStartTimestamp() == null)
            throw new CradleStorageException("Test event must have a start timestamp");
        Instant now = Instant.now();
        if (event.getStartTimestamp().isAfter(now.plusMillis(storeActionRejectionThreshold)))
            throw new CradleStorageException(
                    "Event start timestamp (" + TimeUtils.toLocalTimestamp(event.getStartTimestamp()) +
                            ") is greater than current timestamp ( " + TimeUtils.toLocalTimestamp(now) + " ) plus storeActionRejectionThreshold interval (" + storeActionRejectionThreshold + ")ms");
        validateTestEventEndDate(event);
        if (event.getParentId() != null && !event.getBookId().equals(event.getParentId().getBookId()))
            throw new CradleStorageException("Test event and its parent must be from the same book");

        Set<StoredMessageId> messages = event.getMessages();
        if (messages != null)
            validateMessages(messages, event.getBookId());
    }

    /**
     * Validate that end timestamp of test event is greater than start timestamp
     *
     * @param event to validate
     * @throws CradleStorageException if validation failed
     */
    public static void validateTestEventEndDate(TestEvent event) throws CradleStorageException {
        if (event.getEndTimestamp() != null && event.getEndTimestamp().isBefore(event.getStartTimestamp()))
            throw new CradleStorageException("Test event cannot end sooner than it started");
    }

    /**
     * Serializes test events, skipping non-meaningful or calculable fields
     *
     * @param testEvents to serialize
     * @return array of bytes, containing serialized events
     */
    public static SerializedEntityData<SerializedEntityMetadata> serializeTestEvents(Collection<BatchedStoredTestEvent> testEvents) {
        return serializer.serializeEventBatch(testEvents);
    }

    /**
     * Serializes a single test event.
     *
     * @param testEvent to serialize
     * @return array of bytes, containing serialized event
     */
    public static SerializedEntityData<SerializedEntityMetadata> serializeTestEvent(TestEventSingleToStore testEvent) {
        return serializer.serializeEvent(testEvent);
    }

    /**
     * Deserializes test events from given bytes
     *
     * @param contentBytes to deserialize events from
     * @param id           is batchId
     * @return collection of deserialized test events
     * @throws IOException if deserialization failed
     */
    public static Collection<BatchedStoredTestEvent> deserializeTestEvents(byte[] contentBytes, StoredTestEventId id)
            throws IOException {
        return deserializer.deserializeBatchEntries(contentBytes, new EventBatchCommonParams(id));
    }

    public static Collection<LwBatchedStoredTestEvent> deserializeLwTestEvents(@Nonnull ByteBuffer content, StoredTestEventId id)
            throws IOException {
        content.mark();
        try {
            return deserializer.deserializeLwBatchEntries(content, new EventBatchCommonParams(id));
        } finally {
            content.reset();
        }
    }


    /**
     * Decompresses given ByteBuffer and deserializes test events
     *
     * @param content    to deserialize events from
     * @param eventId    batch id. Required to specify common event params like bookId, scope
     * @param compressed flag that indicates if content needs to be decompressed first
     * @return collection of deserialized test events
     * @throws IOException if deserialization failed
     */
    public static Collection<BatchedStoredTestEvent> bytesToTestEvents(ByteBuffer content, StoredTestEventId eventId, boolean compressed)
            throws IOException, CompressException {
        byte[] contentBytes = getTestEventContentBytes(content, compressed);
        return deserializeTestEvents(contentBytes, eventId);
    }

    public static byte[] getTestEventContentBytes(ByteBuffer content, boolean compressed) throws CompressException {
        byte[] contentBytes = content.array();
        if (!compressed)
            return contentBytes;

        return CompressionType.decompressData(contentBytes);
    }


    /**
     * Returns content of given test event as bytes. If the event is a batch, child events are serialized and returned as bytes
     *
     * @param event whose content to get
     * @return {@link SerializedEntityData} containing test event content.
     */
    public static SerializedEntityData<SerializedEntityMetadata> getTestEventContent(TestEventToStore event) {
        if (event.isBatch()) {
            logger.trace("Serializing children of test event batch '{}'", event.getId());
            return serializeTestEvents(event.asBatch().getTestEvents());
        }
        return serializeTestEvent(event.asSingle());
    }


    public static byte[] serializeLinkedMessageIds(TestEventToStore event) throws IOException {
        if (event.isBatch())
            return EventMessageIdSerializer.serializeBatchLinkedMessageIds(event.asBatch().getBatchMessages());
        return EventMessageIdSerializer.serializeLinkedMessageIds(event.asSingle().getMessages());
    }

    public static Set<StoredMessageId> deserializeLinkedMessageIds(byte[] bytes, BookId bookId) throws IOException {
        return EventMessageIdDeserializer.deserializeLinkedMessageIds(bytes, bookId);
    }

    public static Set<StoredMessageId> deserializeLinkedMessageIds(ByteBuffer buffer, BookId bookId) throws IOException {
        return EventMessageIdDeserializer.deserializeLinkedMessageIds(buffer, bookId);
    }

    public static byte[] serializeLinkedMessageIds(Set<StoredMessageId> messageIds) throws IOException {
        return EventMessageIdSerializer.serializeLinkedMessageIds(messageIds);
    }

    public static Map<StoredTestEventId, Set<StoredMessageId>> deserializeBatchLinkedMessageIds(byte[] bytes, BookId bookId) throws IOException {
        return EventMessageIdDeserializer.deserializeBatchLinkedMessageIds(bytes, bookId);
    }

    public static Map<StoredTestEventId, Set<StoredMessageId>> deserializeBatchLinkedMessageIds(ByteBuffer buffer, BookId bookId) throws IOException {
        return EventMessageIdDeserializer.deserializeBatchLinkedMessageIds(buffer, bookId);
    }

    public static byte[] serializeBatchLinkedMessageIds(Map<StoredTestEventId, Set<StoredMessageId>> ids) throws IOException {
        return EventMessageIdSerializer.serializeBatchLinkedMessageIds(ids);
    }

    private static void validateMessages(Set<StoredMessageId> messages, BookId book) throws CradleStorageException {
        for (StoredMessageId id : messages) {
            if (!id.getBookId().equals(book))
                throw new CradleStorageException("Book of message '" + id + "' differs from test event book (" + book + ")");
        }
    }
}

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
import java.util.zip.DataFormatException;

public class TestEventEntityUtils {

    private static final Logger logger = LoggerFactory.getLogger(TestEventEntityUtils.class);

    public static StoredTestEvent toStoredTestEvent(TestEventEntity testEventEntity, PageId pageId)
            throws IOException, CradleStorageException, DataFormatException, CradleIdException, CompressException {
        StoredTestEventId eventId = createId(testEventEntity, pageId.getBookId());
        logger.trace("Creating test event '{}' from entity", eventId);

        byte[] content = restoreContent(testEventEntity, eventId);
        return testEventEntity.isEventBatch() ? toStoredTestEventBatch(testEventEntity, pageId, eventId, content) : toStoredTestEventSingle(testEventEntity, pageId, eventId, content);
    }


    private static StoredTestEventId createId(TestEventEntity testEventEntity, BookId bookId)
    {
        return new StoredTestEventId(bookId, testEventEntity.getScope(), TestEventEntityUtils.getStartTimestamp(testEventEntity), testEventEntity.getId());
    }

    private static StoredTestEventId createParentId(TestEventEntity testEventEntity) throws CradleIdException
    {
        return StringUtils.isEmpty(testEventEntity.getParentId()) ? null : StoredTestEventId.fromString(testEventEntity.getParentId());
    }


    private static byte[] restoreContent(TestEventEntity testEventEntity, StoredTestEventId eventId) throws CompressException {
        ByteBuffer content = testEventEntity.getContent();
        if (content == null)
            return null;

        byte[] result = content.array();
        if (testEventEntity.isCompressed()) {
            logger.trace("Decompressing content of test event '{}'", eventId);
            return CompressionType.decompressData(result);
        }
        return result;
    }

    private static Set<StoredMessageId> restoreMessages(TestEventEntity testEventEntity, BookId bookId)
            throws IOException {
        ByteBuffer messages = testEventEntity.getMessages();
        if (messages == null)
            return null;

        byte[] result = messages.array();
        return TestEventUtils.deserializeLinkedMessageIds(result, bookId);
    }

    private static Map<StoredTestEventId, Set<StoredMessageId>> restoreBatchMessages(TestEventEntity testEventEntity, BookId bookId)
            throws IOException {
        ByteBuffer messages = testEventEntity.getMessages();
        if (messages == null)
            return null;

        byte[] result = messages.array();
        return TestEventUtils.deserializeBatchLinkedMessageIds(result, bookId);
    }


    private static StoredTestEventSingle toStoredTestEventSingle(TestEventEntity testEventEntity, PageId pageId, StoredTestEventId eventId, byte[] content)
            throws IOException, CradleIdException
    {
        Set<StoredMessageId> messages = restoreMessages(testEventEntity, pageId.getBookId());
        return new StoredTestEventSingle(eventId, testEventEntity.getName(), testEventEntity.getType(), createParentId(testEventEntity),
                TestEventEntityUtils.getEndTimestamp(testEventEntity), testEventEntity.isSuccess(), content, messages, pageId, null, testEventEntity.getRecDate());
    }

    private static StoredTestEventBatch toStoredTestEventBatch(TestEventEntity testEventEntity, PageId pageId, StoredTestEventId eventId, byte[] content)
            throws IOException, CradleStorageException, CradleIdException
    {
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

        logger.debug("Creating entity from test event '{}'", event.getId());

        SerializedEntityData<SerializedEntityMetadata> serializedEntityData = TestEventUtils.getTestEventContent(event);
        byte[] content = serializedEntityData.getSerializedData();
        boolean compressed = false;

        if (content == null) {
            builder.setUncompressedContentSize(0);
            builder.setContentSize(0);
        } else {
            builder.setUncompressedContentSize(content.length);
            if (content.length > maxUncompressedSize) {
                logger.trace("Compressing content of test event '{}'", event.getId());
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
}

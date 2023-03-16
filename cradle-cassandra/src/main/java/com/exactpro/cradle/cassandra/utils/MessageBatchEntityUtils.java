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
package com.exactpro.cradle.cassandra.utils;

import com.exactpro.cradle.BookId;
import com.exactpro.cradle.Direction;
import com.exactpro.cradle.PageId;
import com.exactpro.cradle.cassandra.dao.SerializedEntity;
import com.exactpro.cradle.cassandra.dao.messages.MessageBatchEntity;
import com.exactpro.cradle.messages.MessageBatchToStore;
import com.exactpro.cradle.messages.StoredMessage;
import com.exactpro.cradle.messages.StoredMessageBatch;
import com.exactpro.cradle.messages.StoredMessageId;
import com.exactpro.cradle.serialization.SerializedEntityData;
import com.exactpro.cradle.utils.CompressionUtils;
import com.exactpro.cradle.utils.MessageUtils;
import com.exactpro.cradle.utils.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.util.List;
import java.util.zip.DataFormatException;

public class MessageBatchEntityUtils {
    private static final Logger logger = LoggerFactory.getLogger(MessageBatchEntityUtils.class);

    public static SerializedEntity<MessageBatchEntity> toSerializedEntity(MessageBatchToStore batch,
                                                                          PageId pageId,
                                                                          int maxUncompressedSize) throws IOException
    {
        logger.debug("Creating entity from message batch '{}'", batch.getId());
        MessageBatchEntity.MessageBatchEntityBuilder builder = MessageBatchEntity.builder();

        SerializedEntityData serializedEntityData = MessageUtils.serializeMessages(batch);
        byte[] batchContent = serializedEntityData.getSerializedData();

        builder.setUncompressedContentSize(batchContent.length);
        boolean compressed = batchContent.length > maxUncompressedSize;
        if (compressed) {
            logger.trace("Compressing content of message batch '{}'", batch.getId());
            batchContent = CompressionUtils.compressData(batchContent);
        }
        builder.setContentSize(batchContent.length);

        builder.setBook(pageId.getBookId().getName());
        builder.setPage(pageId.getName());
        StoredMessageId id = batch.getId();
        LocalDateTime ldt = TimeUtils.toLocalTimestamp(id.getTimestamp());
        builder.setFirstMessageDate(ldt.toLocalDate());
        builder.setFirstMessageTime(ldt.toLocalTime());
        builder.setSessionAlias(id.getSessionAlias());
        builder.setDirection(id.getDirection().getLabel());
        builder.setSequence(id.getSequence());
        //Last sequence is used in the getLastSequenceQuery, that returns last chunk
        builder.setLastSequence(batch.getLastMessage().getSequence());

        builder.setFirstMessageTimestamp(batch.getFirstTimestamp());
        builder.setLastMessageTimestamp(batch.getLastTimestamp());
        builder.setMessageCount(batch.getMessageCount());

        builder.setCompressed(compressed);
        //TODO: setLabels(batch.getLabels());
        builder.setContent(ByteBuffer.wrap(batchContent));

        return new SerializedEntity<>(serializedEntityData, builder.build());
    }


    public static StoredMessageBatch toStoredMessageBatch(MessageBatchEntity entity, PageId pageId)
                                                                throws DataFormatException, IOException
    {
        StoredMessageId batchId = createId(entity, pageId.getBookId());
        logger.debug("Creating message batch '{}' from entity", batchId);

        byte[] content = restoreContent(entity, batchId);
        List<StoredMessage> storedMessages = MessageUtils.deserializeMessages(content, batchId);
        return new StoredMessageBatch(storedMessages, pageId, entity.getRecDate());
    }


    private static StoredMessageId createId(MessageBatchEntity entity, BookId bookId) {
        return new StoredMessageId(bookId,
                                    entity.getSessionAlias(),
                                    Direction.byLabel(entity.getDirection()),
                                    TimeUtils.toInstant(entity.getFirstMessageDate(), entity.getFirstMessageTime()),
                                    entity.getSequence());
    }


    private static byte[] restoreContent(MessageBatchEntity entity, StoredMessageId messageBatchId)
                                                                throws DataFormatException, IOException
    {
        ByteBuffer content = entity.getContent();
        if (content == null)
            return null;

        byte[] result = content.array();
        if (entity.isCompressed()) {
            logger.trace("Decompressing content of message batch '{}'", messageBatchId);
            return CompressionUtils.decompressData(result);
        }
        return result;
    }
}

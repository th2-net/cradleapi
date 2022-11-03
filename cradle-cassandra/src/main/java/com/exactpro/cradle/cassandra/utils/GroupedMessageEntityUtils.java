/*
 * Copyright 2021-2022 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.cradle.PageId;
import com.exactpro.cradle.cassandra.dao.SerializedEntity;
import com.exactpro.cradle.cassandra.dao.messages.GroupedMessageBatchEntity;
import com.exactpro.cradle.messages.GroupedMessageBatchToStore;
import com.exactpro.cradle.messages.StoredGroupedMessageBatch;
import com.exactpro.cradle.messages.StoredMessage;
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

public class GroupedMessageEntityUtils {

    private static final Logger logger = LoggerFactory.getLogger(GroupedMessageEntityUtils.class);

    public static SerializedEntity<GroupedMessageBatchEntity> toSerializedEntity (GroupedMessageBatchToStore batch, PageId pageId, int maxUncompressedSize)
            throws IOException	{

        GroupedMessageBatchEntity.GroupedMessageBatchEntityBuilder builder = GroupedMessageBatchEntity.builder();

        String group = batch.getGroup();

        logger.debug("Creating entity from grouped message batch '{}'", group);

        SerializedEntityData serializedEntityData = MessageUtils.serializeMessages(batch.getMessages());

        byte[] batchContent = serializedEntityData.getSerializedData();
        boolean compressed = batchContent.length > maxUncompressedSize;
        if (compressed)	{
            logger.trace("Compressing content of grouped message batch '{}'", group);
            batchContent = CompressionUtils.compressData(batchContent);
        }

        builder.setGroup(group);
        builder.setBook(pageId.getBookId().getName());
        builder.setPage(pageId.getName());

        LocalDateTime firstDateTime = TimeUtils.toLocalTimestamp(batch.getFirstTimestamp());
        builder.setFirstMessageDate(firstDateTime.toLocalDate());
        builder.setFirstMessageTime(firstDateTime.toLocalTime());

        LocalDateTime lastDateTime = TimeUtils.toLocalTimestamp(batch.getLastTimestamp());
        builder.setLastMessageDate(lastDateTime.toLocalDate());
        builder.setLastMessageTime(lastDateTime.toLocalTime());

        builder.setMessageCount(batch.getMessageCount());

        builder.setCompressed(compressed);
        //TODO: setLabels(batch.getLabels());
        builder.setContent(ByteBuffer.wrap(batchContent));

        return new SerializedEntity<>(serializedEntityData, builder.build());
    }

    public static StoredGroupedMessageBatch toStoredGroupedMessageBatch(GroupedMessageBatchEntity entity, PageId pageId)	throws DataFormatException, IOException
    {
        logger.debug("Creating grouped message batch from entity");

        byte[] content = restoreContent(entity, entity.getGroup());
        List<StoredMessage> storedMessages = MessageUtils.deserializeMessages(content, pageId.getBookId());
        return new StoredGroupedMessageBatch(entity.getGroup(), storedMessages, pageId, entity.getRecDate());
    }

    private static byte[] restoreContent(GroupedMessageBatchEntity entity, String group) throws DataFormatException, IOException {
        ByteBuffer content = entity.getContent();
        if (content == null)
            return null;

        byte[] result = content.array();
        if (entity.isCompressed()) {
            logger.trace("Decompressing content of grouped message batch '{}'", group);
            return CompressionUtils.decompressData(result);
        }
        return result;
    }
}

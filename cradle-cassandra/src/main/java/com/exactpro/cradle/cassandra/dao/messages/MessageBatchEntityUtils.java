package com.exactpro.cradle.cassandra.dao.messages;

import com.exactpro.cradle.BookId;
import com.exactpro.cradle.Direction;
import com.exactpro.cradle.PageId;
import com.exactpro.cradle.messages.MessageBatch;
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
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.List;
import java.util.zip.DataFormatException;

public class MessageBatchEntityUtils {

    private static final Logger logger = LoggerFactory.getLogger(MessageBatchEntityUtils.class);

    public static MessageBatchEntity fromMessageBatch(MessageBatch batch, PageId pageId, int maxUncompressedSize) throws IOException
    {
        logger.debug("Creating entity from message batch '{}'", batch.getId());
        MessageBatchEntity.MessageBatchEntityBuilder builder = MessageBatchEntity.MessageBatchEntityBuilder.builder();

        SerializedEntityData serializedEntityData = MessageUtils.serializeMessages(batch.getMessages());

        byte[] batchContent = serializedEntityData.getSerializedData();
        boolean compressed = batchContent.length > maxUncompressedSize;
        if (compressed)
        {
            logger.trace("Compressing content of message batch '{}'", batch.getId());
            batchContent = CompressionUtils.compressData(batchContent);
        }

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

        setFirstMessageTimestamp(builder, batch.getFirstTimestamp());
        setLastMessageTimestamp(builder, batch.getLastTimestamp());
        builder.setMessageCount(batch.getMessageCount());

        builder.setCompressed(compressed);
        //TODO: setLabels(batch.getLabels());
        builder.setContent(ByteBuffer.wrap(batchContent));

        return builder.build();
    }

    private static void setLastMessageTimestamp(MessageBatchEntity.MessageBatchEntityBuilder builder, Instant timestamp)
    {
        LocalDateTime ldt = TimeUtils.toLocalTimestamp(timestamp);
        builder.setLastMessageDate(ldt.toLocalDate());
        builder.setLastMessageTime(ldt.toLocalTime());
    }

    private static void setFirstMessageTimestamp(MessageBatchEntity.MessageBatchEntityBuilder builder, Instant timestamp)
    {
        LocalDateTime ldt = TimeUtils.toLocalTimestamp(timestamp);
        builder.setFirstMessageDate(ldt.toLocalDate());
        builder.setFirstMessageTime(ldt.toLocalTime());
    }

    public static Instant getFirstMessageTimestamp(MessageBatchEntity entity)
    {
        return TimeUtils.toInstant(entity.getFirstMessageDate(), entity.getFirstMessageTime());
    }

    public static Instant getLastMessageTimestamp(MessageBatchEntity entity)
    {
        return TimeUtils.toInstant(entity.getLastMessageDate(), entity.getLastMessageTime());
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

    public static StoredMessageId createId(MessageBatchEntity entity, BookId bookId)
    {
        return new StoredMessageId(bookId, entity.getSessionAlias(), Direction.byLabel(entity.getDirection()),
                TimeUtils.toInstant(entity.getFirstMessageDate(), entity.getFirstMessageTime()), entity.getSequence());
    }

    private static byte[] restoreContent(MessageBatchEntity entity, StoredMessageId messageBatchId)
            throws DataFormatException, IOException
    {
        ByteBuffer content = entity.getContent();
        if (content == null)
            return null;

        byte[] result = content.array();
        if (entity.isCompressed())
        {
            logger.trace("Decompressing content of message batch '{}'", messageBatchId);
            return CompressionUtils.decompressData(result);
        }
        return result;
    }
}

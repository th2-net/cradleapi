package com.exactpro.cradle.messages;

import com.exactpro.cradle.Direction;
import com.exactpro.cradle.serialization.MessagesSizeCalculator;
import com.exactpro.cradle.utils.CradleStorageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.*;

/**
 * Base class for Stored Message Batches
 */
public abstract class AbstractStoredMessageBatch {
    private static final Logger logger = LoggerFactory.getLogger(AbstractStoredMessageBatch.class);

    public static final int DEFAULT_MAX_BATCH_SIZE = 1024*1024;  //1 Mb
    protected static final long EMPTY_BATCH_SIZE = MessagesSizeCalculator.calculateServiceMessageBatchSize(null);

    protected final long maxBatchSize;
    protected long batchSize;
    protected final List<StoredMessage> messages;

    public AbstractStoredMessageBatch()
    {
        this(DEFAULT_MAX_BATCH_SIZE);
    }

    public AbstractStoredMessageBatch(long maxBatchSize)
    {
        this.messages = createMessagesList();
        this.maxBatchSize = maxBatchSize;
    }

    /**
     * @return number of messages currently stored in the batch
     */
    public int getMessageCount()
    {
        return messages.size();
    }

    /**
     * @return size of messages currently stored in the batch
     */
    public long getBatchSize()
    {
        return batchSize == 0 ? EMPTY_BATCH_SIZE : batchSize;
    }

    /**
     * @return collection of messages stored in the batch
     */
    public Collection<StoredMessage> getMessages()
    {
        return new ArrayList<>(messages);
    }

    /**
     * @return collection of messages stored in the batch in reverse order
     */
    public Collection<StoredMessage> getMessagesReverse()
    {
        List<StoredMessage> list = new ArrayList<>(messages);
        Collections.reverse(list);

        return list;
    }

    /**
     * @return Duration between first and last message timestamps stored in the batch
     */
    public Duration getBatchDuration()
    {
        if (messages.size()<2)
            return Duration.ZERO;

        return Duration.between(messages.get(0).getTimestamp(), messages.get(getMessageCount() - 1).getTimestamp());
    }

    /**
     * Adds message to the batch. Batch will add correct message ID by itself, verifying message to match batch conditions.
     * Messages can be added to batch until {@link #isFull()} returns true.
     * Result of this method should be used for all further operations on the message
     * @param message to add to the batch
     * @return immutable message object with assigned ID
     * @throws CradleStorageException if message cannot be added to the batch due to verification failure or if batch limit is reached
     */
    public StoredMessage addMessage(MessageToStore message) throws CradleStorageException
    {
        int messageSize = calculateSizeAndCheckConstraints(message);

        return addMessageInternal(message, messageSize);
    }

    protected abstract int calculateSizeAndCheckConstraints(MessageToStore message) throws CradleStorageException;

    protected abstract StoredMessage addMessageInternal(MessageToStore message, int expectedMessageSize);

    public StoredMessage getFirstMessage()
    {
        return !messages.isEmpty() ? messages.get(0) : null;
    }

    public StoredMessage getLastMessage()
    {
        return !messages.isEmpty() ? messages.get(messages.size()-1) : null;
    }

    /**
     * @return timestamp of first message within the batch
     */
    public Instant getFirstTimestamp()
    {
        StoredMessage m = getFirstMessage();
        return m != null ? m.getTimestamp() : null;
    }

    /**
     * @return timestamp of last message within the batch
     */
    public Instant getLastTimestamp()
    {
        StoredMessage m = getLastMessage();
        return m != null ? m.getTimestamp() : null;
    }

    /**
     * @return true if no messages were added to batch yet
     */
    public boolean isEmpty()
    {
        return messages.size() == 0;
    }

    /**
     * Indicates if the batch cannot hold more messages
     * @return true if batch capacity is reached and the batch must be flushed to Cradle
     */
    public boolean isFull()
    {
        return getBatchSize() >= maxBatchSize;
    }

    /**
     * Shows how many bytes the batch can hold till its capacity is reached
     * @return number of bytes the batch can hold
     */
    public long getSpaceLeft()
    {
        long result = maxBatchSize-getBatchSize();
        return result > 0 ? result : 0;
    }

    /**
     * Shows if batch has enough space to hold given message
     * @param message to check against batch capacity
     * @return true if batch has enough space to hold given message
     */
    public boolean hasSpace(MessageToStore message)
    {
        return hasSpace(MessagesSizeCalculator.calculateMessageSizeInBatch(message), message.getStreamName());
    }

    /**
     * Shows if batch has enough space to hold given message
     * @param expected expected size of given message
     * @param streamName stream name of expected message
     * @return true if batch has enough space to hold given message
     */
    protected boolean hasSpace(int expected, String streamName)
    {
        long currentSize = messages.isEmpty()
                ? MessagesSizeCalculator.calculateServiceMessageBatchSize(streamName) : getBatchSize();
        return currentSize + expected <= maxBatchSize;
    }

    protected List<StoredMessage> createMessagesList()
    {
        return new ArrayList<>();
    }
}

/*
 * Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.cradle.messages;

import java.time.Instant;
import java.util.*;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.cradle.Direction;
import com.exactpro.cradle.utils.CradleStorageException;
import com.exactpro.cradle.utils.MessageUtils;

/**
 * Holds information about batch of messages stored in Cradle.
 * All messages stored in the batch should be from one sequence and should be related to the same batch, heving the same direction.
 * ID of first message is treated as batch ID.
 * Batch has limited capacity. If batch is full, messages can't be added to it and the batch must be flushed to Cradle
 */
public class StoredMessageBatch
{
	private static final Logger logger = LoggerFactory.getLogger(StoredMessageBatch.class);
	
	public static int MAX_MESSAGES_COUNT = 100,
			MAX_MESSAGES_SIZE = 1024*1024;  //1 Mb
	
	private StoredMessageBatchId id;
	private int storedMessagesCount = 0;
	private long storedMessagesSize = 0;
	private final StoredMessage[] messages;
	
	public StoredMessageBatch()
	{
		this.messages = new StoredMessage[MAX_MESSAGES_COUNT];
	}
	
	
	public static StoredMessageBatch singleton(MessageToStore message) throws CradleStorageException
	{
		StoredMessageBatch result = new StoredMessageBatch();
		result.addMessage(message);
		return result;
	}
	
	
	/**
	 * @return batch ID. It is based on first message in the batch
	 */
	public StoredMessageBatchId getId()
	{
		return id;
	}
	
	/**
	 * @return directions of messages in the batch
	 */
	public Direction getDirection()
	{
		return id != null ? id.getDirection() : null;
	}
	
	/**
	 * @return name of stream all messages in the batch are related to
	 */
	public String getStreamName()
	{
		return id != null ? id.getStreamName() : null;
	}
	
	/**
	 * @return number of messages currently stored in the batch
	 */
	public int getMessageCount()
	{
		return storedMessagesCount;
	}
	
	/**
	 * @return size of messages content currently stored in the batch
	 */
	public long getMessagesSize()
	{
		return storedMessagesSize;
	}
	
	/**
	 * @return collection of messages stored in the batch
	 */
	public Collection<StoredMessage> getMessages()
	{
		List<StoredMessage> result = new ArrayList<>();
		for (int i = 0; i < storedMessagesCount; i++)
			result.add(messages[i]);
		return result;
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
		if (isFull())
			throw new CradleStorageException("Batch is full");
		
		MessageUtils.validateMessage(message);
		
		long messageIndex;
		if (id == null)
		{
			String sm = message.getStreamName();
			Direction d = message.getDirection();
			long i = message.getIndex();
			if (StringUtils.isEmpty(sm))
				throw new CradleStorageException("Stream name for first message in batch cannot be empty");
			if (d == null)
				throw new CradleStorageException("Message direction for first message in batch must be set");
			if (i < 0)
				throw new CradleStorageException("Message index for first message in batch cannot be negative");
			
			id = new StoredMessageBatchId(sm, d, i);
			messageIndex = message.getIndex();
		}
		else
		{
			if (!id.getStreamName().equals(message.getStreamName()))
				throw new CradleStorageException("Batch contains messages of stream with name '"+id.getStreamName()+"', but in your message it is '"+message.getStreamName()+"'");
			if (id.getDirection() != message.getDirection())
				throw new CradleStorageException("Batch contains messages with direction "+id.getDirection()+", but in your message it is "+message.getDirection());
			
			StoredMessage lastMsg = getLastMessage();
			if (message.getIndex() > 0)  //I.e. message index is set
			{
				messageIndex = message.getIndex();
				if (messageIndex <= lastMsg.getIndex())
					throw new CradleStorageException("Message index should be greater than "+lastMsg.getIndex()+" for the batch to contain sequenced messages, but in your message it is "+messageIndex);
				if (messageIndex != lastMsg.getIndex()+1)
					logger.warn("Message index should be "+(lastMsg.getIndex()+1)+" for the batch to contain strictly sequenced messages, but in your message it is "+messageIndex);
			}
			else
				messageIndex = lastMsg.getIndex()+1;
		}
		
		StoredMessage msg = new StoredMessage(message, new StoredMessageId(message.getStreamName(), message.getDirection(), messageIndex));
		messages[storedMessagesCount++] = msg;
		if (msg.getContent() != null)
			storedMessagesSize += msg.getContent().length;
		return msg;
	}
	
	
	
	public StoredMessage getFirstMessage()
	{
		return storedMessagesCount > 0 ? messages[0] : null;
	}
	
	public StoredMessage getLastMessage()
	{
		return storedMessagesCount > 0 ? messages[storedMessagesCount-1] : null;
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
		return storedMessagesCount == 0;
	}
	
	/**
	 * Indicates if the batch cannot hold more messages
	 * @return true if batch capacity is reached and the batch must be flushed to Cradle
	 */
	public boolean isFull()
	{
		return storedMessagesCount >= messages.length || storedMessagesSize >= MAX_MESSAGES_SIZE;
	}

    /**
     *
     * @param batch the batch to add to the current one.
     *              The batch to add must contains message with same stream name and direction as the current one.
     *              The index of the first message in the [batch] should be greater
     *              than the last message index in the current batch.
     * @return true if the result batch meets the restriction for message count and batch size
     * @throws CradleStorageException if the batch doesn't meet the requirements regarding inner content
     */
    public boolean addBatch(StoredMessageBatch batch) throws CradleStorageException {
        if (batch.isEmpty()) {
            // we don't need to actually add empty batch
            return true;
        }
        if (isEmpty()) {
            this.id = batch.id;
            this.storedMessagesCount = batch.storedMessagesCount;
            this.storedMessagesSize = batch.storedMessagesSize;
            System.arraycopy(batch.messages, 0, this.messages, 0, batch.storedMessagesCount);
            return true;
        }
        if (isFull() || batch.isFull()) {
            return false;
        }
        int resultCount = storedMessagesCount + batch.storedMessagesCount;
        long resultSize = storedMessagesSize + batch.storedMessagesSize;
        if (resultCount > MAX_MESSAGES_COUNT || resultSize > MAX_MESSAGES_SIZE) {
            // cannot add because of size or count limit
            return false;
        }
        verifyBatch(batch);
        System.arraycopy(batch.messages, 0, messages, storedMessagesCount, batch.storedMessagesCount);
        this.storedMessagesCount = resultCount;
        this.storedMessagesSize = resultSize;
        return true;
    }

    private void verifyBatch(StoredMessageBatch otherBatch) throws CradleStorageException {
        StoredMessageBatchId otherId = otherBatch.id;
        if (!Objects.equals(id.getStreamName(), otherId.getStreamName())
                || id.getDirection() != otherId.getDirection()) {
            throw new CradleStorageException(String.format("IDs are not compatible. Current id: %s, Other id: %s", id, otherId));
        }
        long currentLastIndex = getLastMessage().getIndex();
        long otherFirstIndex = otherBatch.getFirstMessage().getIndex();
        if (currentLastIndex >= otherFirstIndex) {
            throw new CradleStorageException(String.format("Batches are not ordered. Current last index: %d; Other first index: %d", currentLastIndex, otherFirstIndex));
        }
    }
}

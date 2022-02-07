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

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.cradle.Direction;
import com.exactpro.cradle.utils.CradleStorageException;
import com.exactpro.cradle.utils.MessageUtils;

/**
 * Holds information about batch of messages stored in Cradle.
 * All messages stored in the batch should be from one sequence and should be related to the same batch, having the same direction.
 * ID of first message is treated as batch ID.
 * Batch has limited capacity. If batch is full, messages can't be added to it and the batch must be flushed to Cradle
 */
public class StoredMessageBatch
{
	private static final Logger logger = LoggerFactory.getLogger(StoredMessageBatch.class);
	
	public static final int DEFAULT_MAX_BATCH_SIZE = 1024*1024;  //1 Mb
	
	private StoredMessageBatchId id;
	private final long maxBatchSize;
	private long batchSize = 0;
	private final List<StoredMessage> messages;
	
	public StoredMessageBatch()
	{
		this.messages = createMessagesList();
		this.maxBatchSize = DEFAULT_MAX_BATCH_SIZE;
	}
	
	public StoredMessageBatch(long maxBatchSize)
	{
		this.messages = createMessagesList();
		this.maxBatchSize = maxBatchSize;
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
		return messages.size();
	}
	
	/**
	 * @return size of messages currently stored in the batch
	 */
	public long getBatchSize()
	{
		return batchSize;
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
	 * Adds message to the batch. Batch will add correct message ID by itself, verifying message to match batch conditions.
	 * Messages can be added to batch until {@link #isFull()} returns true.
	 * Result of this method should be used for all further operations on the message
	 * @param message to add to the batch
	 * @return immutable message object with assigned ID
	 * @throws CradleStorageException if message cannot be added to the batch due to verification failure or if batch limit is reached
	 */
	public StoredMessage addMessage(MessageToStore message) throws CradleStorageException
	{
		checkBatchConstraints(message);

		return addMessageInternal(message);
	}

	protected void checkBatchConstraints(MessageToStore message) throws CradleStorageException
	{
		if (!hasSpace(message))
			throw new CradleStorageException("Batch has not enough space to hold given message");
		
		MessageUtils.validateMessage(message);
		
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
		}
		else
		{
			if (!id.getStreamName().equals(message.getStreamName()))
				throw new CradleStorageException("Batch contains messages of stream with name '" + id.getStreamName() +
						"', but in your message it is '" + message.getStreamName() + "'");
			if (id.getDirection() != message.getDirection())
				throw new CradleStorageException("Batch contains messages with direction " + id.getDirection() +
						", but in your message it is " + message.getDirection());

			StoredMessage lastMsg = getLastMessage();
			if (message.getIndex() > 0)  //I.e. message index is set
			{
				long messageIndex = message.getIndex();
				if (messageIndex <= lastMsg.getIndex())
					throw new CradleStorageException("Message index should be greater than "+lastMsg.getIndex()+
							" for the batch to contain sequenced messages, but in your message it is "+messageIndex);
				if (messageIndex != lastMsg.getIndex()+1)
					logger.debug("Message index should be "+(lastMsg.getIndex()+1)+
							" for the batch to contain strictly sequenced messages, but in your message it is "+messageIndex);
			}
			if (lastMsg.getTimestamp().isAfter(message.getTimestamp()))
				throw new CradleStorageException(
						"Message timestamp should be greater than last message timestamp in batch '" + lastMsg.getTimestamp()
								+ "' but in your message it is '" + message.getTimestamp() + "'");
		}
	}

	protected StoredMessage addMessageInternal(MessageToStore message)
	{
		long messageIndex = message.getIndex() >= 0 ? message.getIndex() : getLastMessage().getIndex()+1;
		if (id == null)
			id = new StoredMessageBatchId(message.getStreamName(), message.getDirection(), messageIndex);
		StoredMessage msg = new StoredMessage(message, new StoredMessageId(message.getStreamName(), message.getDirection(), messageIndex));
		messages.add(msg);
		batchSize += msg.getContent().length;
		return msg;
	}
	
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
		return batchSize >= maxBatchSize;
	}
	
	/**
	 * Shows how many bytes the batch can hold till its capacity is reached
	 * @return number of bytes the batch can hold
	 */
	public long getSpaceLeft()
	{
		long result = maxBatchSize-batchSize;
		return result > 0 ? result : 0;
	}
	
	/**
	 * Shows if batch has enough space to hold given message
	 * @param message to check against batch capacity
	 * @return true if batch has enough space to hold given message
	 */
	public boolean hasSpace(MessageToStore message)
	{
		byte[] content = message.getContent();
		return ArrayUtils.isEmpty(content) || batchSize+content.length <= maxBatchSize;
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
			this.batchSize = batch.batchSize;
			messages.addAll(batch.messages);
			return true;
		}
		if (isFull() || batch.isFull()) {
			return false;
		}
		long resultSize = batchSize + batch.batchSize;
		if (resultSize > maxBatchSize) {
			// cannot add because of size limit
			return false;
		}
		verifyBatch(batch);
		messages.addAll(batch.messages);
		this.batchSize = resultSize;
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
	
	protected List<StoredMessage> createMessagesList()
	{
		return new ArrayList<>();
	}
}

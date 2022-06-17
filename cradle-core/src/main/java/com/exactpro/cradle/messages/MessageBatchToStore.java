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

package com.exactpro.cradle.messages;

import java.time.Instant;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.*;

import com.exactpro.cradle.serialization.MessagesSizeCalculator;
import com.exactpro.cradle.utils.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.cradle.utils.CradleStorageException;

/**
 * Holds information about batch of messages to be stored in Cradle.
 * All messages stored in the batch should form a sequence and should be related to the same session, having the same direction.
 * ID of first message is treated as batch ID.
 */
public class MessageBatchToStore extends StoredMessageBatch
{
	private static final Logger logger = LoggerFactory.getLogger(MessageBatchToStore.class);
	
	private final int maxBatchSize;
	private LocalDate batchDate;
	
	public MessageBatchToStore(int maxBatchSize)
	{
		this.maxBatchSize = maxBatchSize;
	}
	
	public static MessageBatchToStore singleton(MessageToStore message, int maxBatchSize) throws CradleStorageException
	{
		MessageBatchToStore result = new MessageBatchToStore(maxBatchSize);
		result.addMessage(message);
		return result;
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
	public int getSpaceLeft()
	{
		int result = maxBatchSize - batchSize;
		return Math.max(result, 0);
	}
	
	/**
	 * Shows if batch has enough space to hold given message
	 * @param message to check against batch capacity
	 * @return true if batch has enough space to hold given message
	 */
	public boolean hasSpace(MessageToStore message)
	{
		return hasSpace(MessagesSizeCalculator.calculateMessageSize(message));
	}

	private boolean hasSpace(int messageSize)
	{
		return batchSize + messageSize <= maxBatchSize;
	}
	

	/**
	 * Adds message to the batch. Batch will add correct message ID by itself, verifying message to match batch conditions.
	 * Result of this method should be used for all further operations on the message
	 * @param message to add to the batch
	 * @return immutable message object with assigned ID
	 * @throws CradleStorageException if message cannot be added to the batch due to verification failure
	 */
	public StoredMessage addMessage(MessageToStore message) throws CradleStorageException
	{
		int expMsgSize = MessagesSizeCalculator.calculateMessageSizeInBatch(message);
		if (!hasSpace(expMsgSize))
			throw new CradleStorageException("Batch has not enough space to hold given message");
		
		// Checking that the timestamp of a message is not from the future
		// Other checks have already been done when the MessageToStore was created
		Instant now = Instant.now();
		if (message.getTimestamp().isAfter(now))
			throw new CradleStorageException(
					"Message timestamp (" + TimeUtils.toLocalTimestamp(message.getTimestamp()) +
							") is greater than current timestamp (" + TimeUtils.toLocalTimestamp(now) + ")");

		long messageSeq;
		if (id == null)
		{
			long i = message.getSequence();
			if (i < 0)
				throw new CradleStorageException("Sequence number for first message in batch cannot be negative");
			
			id = new StoredMessageId(message.getBookId(), message.getSessionAlias(), message.getDirection(), message.getTimestamp(), i);
			batchDate = TimeUtils.toLocalTimestamp(id.getTimestamp()).toLocalDate();
			messageSeq = i;
		}
		else
		{
			if (!id.getBookId().equals(message.getBookId()))
				throw new CradleStorageException("Batch contains messages of book '"+id.getBookId()+"', "
						+ "but in your message it is '"+message.getBookId()+"'");
			if (!id.getSessionAlias().equals(message.getSessionAlias()))
				throw new CradleStorageException("Batch contains messages of session '"+id.getSessionAlias()+"', "
						+ "but in your message it is '"+message.getSessionAlias()+"'");
			if (id.getDirection() != message.getDirection())
				throw new CradleStorageException("Batch contains messages with direction "+id.getDirection()+", "
						+ "but in your message it is "+message.getDirection());
			LocalDate messageDate = TimeUtils.toLocalTimestamp(message.getTimestamp()).toLocalDate();
			if (!batchDate.equals(messageDate))
				throw new CradleStorageException("Batch contains messages with date '" + batchDate + "', "
						+ "but in your message it is '" + messageDate);
			StoredMessage lastMsg = getLastMessage();
			
			if (lastMsg.getTimestamp().isAfter(message.getTimestamp()))
				throw new CradleStorageException("Message timestamp should not be before "+lastMsg.getTimestamp()+", "
						+ "but in your message it is "+message.getTimestamp());

			if (message.getSequence() > 0)  //I.e. message sequence is set
			{
				messageSeq = message.getSequence();
				if (messageSeq <= lastMsg.getSequence())
					throw new CradleStorageException("Sequence number should be greater than "+lastMsg.getSequence()+" "
							+ "for the batch to contain sequenced messages, but in your message it is "+messageSeq);
				if (messageSeq != lastMsg.getSequence()+1)
					logger.warn("Sequence number should be "+(lastMsg.getSequence()+1)+" "
							+ "for the batch to contain strictly sequenced messages, but in your message it is "+messageSeq);
			}
			else
				messageSeq = lastMsg.getSequence()+1;
		}
		
		StoredMessageId msgId = new StoredMessageId(message.getBookId(), message.getSessionAlias(), message.getDirection(), message.getTimestamp(), messageSeq);
		StoredMessage msg = new StoredMessage(message, msgId, null);
		messages.add(msg);
		batchSize += expMsgSize;

		return msg;
	}

  /**
   *
   * @param batch the batch to add to the current one.
   *              The batch to add must contain message with same stream name and direction as the current one.
   *              The index of the first message in the [batch] should be greater
   *              than the last message index in the current batch.
   * @return true if the result batch meets the restriction for message count and batch size
   * @throws CradleStorageException if the batch doesn't meet the requirements regarding inner content
   */
	public boolean addBatch(MessageBatchToStore batch) throws CradleStorageException {
		if (batch.isEmpty()) {
			// we don't need to actually add empty batch
			return true;
		}
		if (isEmpty()) {
			this.id = batch.getId();
			this.batchSize = batch.getBatchSize();
			messages.addAll(batch.getMessages());
			return true;
		}
		if (isFull() || batch.isFull()) {
			return false;
		}
		int resultSize = batchSize + batch.messages.stream().mapToInt(MessagesSizeCalculator::calculateMessageSizeInBatch).sum();

		if (resultSize > maxBatchSize) {
			// cannot add because of size limit
			return false;
		}
		verifyBatch(batch);
		messages.addAll(batch.getMessages());
		this.batchSize = resultSize;
		return true;
	}
	
	private void verifyBatch(MessageBatch otherBatch) throws CradleStorageException {
		StoredMessageId otherId = otherBatch.getId();
		if (!Objects.equals(id.getBookId(), otherId.getBookId())
				|| !Objects.equals(id.getSessionAlias(), otherId.getSessionAlias())
				|| id.getDirection() != otherId.getDirection()) {
			throw new CradleStorageException(String.format("IDs are not compatible. Current id: %s, Other id: %s", id, otherId));
		}
		
		StoredMessage lastMsg = getLastMessage(),
				otherFirstMsg = otherBatch.getFirstMessage();
		
		Instant currentLastTimestamp = lastMsg.getTimestamp(),
				otherFirstTimestamp = otherFirstMsg.getTimestamp();
		if (currentLastTimestamp.isAfter(otherFirstTimestamp))
			throw new CradleStorageException(String.format("Batches are not ordered. Current last timestamp: %s; Other first timestamp: %s", 
					currentLastTimestamp, otherFirstTimestamp));
		
		long currentLastSeq = lastMsg.getSequence();
		long otherFirstSeq = otherFirstMsg.getSequence();
		if (currentLastSeq >= otherFirstSeq) {
			throw new CradleStorageException(String.format("Batches are not ordered. "
					+ "Current last sequence number: %d; Other first sequence number: %d", currentLastSeq, otherFirstSeq));
		}
	}
}
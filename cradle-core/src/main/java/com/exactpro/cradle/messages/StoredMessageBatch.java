/******************************************************************************
 * Copyright (c) 2009-2020, Exactpro Systems LLC
 * www.exactpro.com
 * Build Software to Test Software
 *
 * All rights reserved.
 * This is unpublished, licensed software, confidential and proprietary 
 * information which is the property of Exactpro Systems LLC or its licensors.
 ******************************************************************************/

package com.exactpro.cradle.messages;

import java.time.Instant;
import java.util.*;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.cradle.Direction;
import com.exactpro.cradle.messages.StoredMessage;
import com.exactpro.cradle.messages.StoredMessageId;
import com.exactpro.cradle.utils.CradleStorageException;

/**
 * Holds information about batch of messages stored in Cradle.
 * All messages stored in the batch should be from one sequence and should be related to the same batch, heving the same direction.
 * ID of first message is treated as batch ID.
 * Batch has limited capacity. If batch is full, messages can't be added to it and the batch must be flushed to Cradle
 */
public class StoredMessageBatch
{
	private static final Logger logger = LoggerFactory.getLogger(StoredMessageBatch.class);
	
	public static int MAX_MESSAGES_NUMBER = 10;
	
	private StoredMessageBatchId id;
	private int storedMessagesCount = 0;
	private final StoredMessage[] messages;
	
	public StoredMessageBatch()
	{
		this.messages = new StoredMessage[MAX_MESSAGES_NUMBER];
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
		
		StoredMessage msg = new StoredMessage(message, new StoredMessageId(id, messageIndex));
		messages[storedMessagesCount++] = msg;
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
		return storedMessagesCount >= messages.length;
	}
}
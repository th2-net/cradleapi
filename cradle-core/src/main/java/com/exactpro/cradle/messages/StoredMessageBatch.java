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

import com.exactpro.cradle.Direction;
import com.exactpro.cradle.messages.StoredMessage;
import com.exactpro.cradle.messages.StoredMessageId;
import com.exactpro.cradle.utils.CradleStorageException;

/**
 * Holds information about batch of messages stored in Cradle.
 * All messages stored in the batch are linked to its ID
 * Batch has limited capacity. If batch is full, messages can't be added to it and the batch must be flushed to Cradle
 */
public class StoredMessageBatch
{
	public static int MAX_MESSAGES_NUMBER = 10;
	
	private final StoredMessageBatchId id;
	private int storedMessagesCount = 0;
	private final StoredMessage[] messages;
	private Direction messagesDirections;
	private final Map<String, Set<StoredMessageId>> streamsMessages;

	public StoredMessageBatch(StoredMessageBatchId id)
	{
		this.id = id;
		this.messages = new StoredMessage[MAX_MESSAGES_NUMBER];
		this.streamsMessages = new HashMap<>();
	}
	
	
	public static StoredMessageBatch singleton(StoredMessage message) throws CradleStorageException
	{
		StoredMessageBatch result = new StoredMessageBatch(message.getId().getBatchId());
		result.addMessage(message);
		return result;
	}
	

	public StoredMessageBatchId getId()
	{
		return id;
	}

	public int getStoredMessagesCount()
	{
		return storedMessagesCount;
	}

	public StoredMessage[] getMessages()
	{
		return messages;
	}
	
	public List<StoredMessage> getMessagesList()
	{
		List<StoredMessage> result = new ArrayList<>();
		for (int i = 0; i < storedMessagesCount; i++)
			result.add(messages[i]);
		return result;
	}

	/**
	 * Adds message to the batch. If message ID is not specified, batch will add correct ID by itself, else message ID will be verified to match batch conditions.
	 * Messages can be added to batch until {@link #isFull()} returns true. 
	 * @param message to add to batch
	 * @return ID assigned to added message or ID specified in message, if present
	 * @throws CradleStorageException if message cannot be added to batch due to ID verification failure or if batch limit is reached
	 */
	public StoredMessageId addMessage(StoredMessage message) throws CradleStorageException
	{
		if (isFull())
			throw new CradleStorageException("Batch is full");
		
		if (message.getId() != null)
		{
			StoredMessageId id = message.getId();
			if (!id.getBatchId().equals(id))
				throw new CradleStorageException("Batch ID in message ("+id.getBatchId()+") doesn't match with ID of batch it is being added to ("+id+")");
			if (id.getIndex() != storedMessagesCount)
				throw new CradleStorageException("Unexpected message index - "+id.getIndex()+". Expected "+storedMessagesCount);
		}
		else
			message.setId(new StoredMessageId(id, storedMessagesCount));
		
		messages[storedMessagesCount++] = message;
		setDirection(message);
		mapToStream(message);
		
		return message.getId();
	}
	
	/**
	 * @return timestamp of first message within the batch
	 */
	public Instant getTimestamp()
	{
		if (storedMessagesCount > 0)
			return messages[0].getTimestamp();
		return null;
	}
	
	/**
	 * @return number of messages currently stored in the batch
	 */
	public int getMessageCount()
	{
		return storedMessagesCount;
	}
	
	/**
	 * @return true if no messages were added to batch yet
	 */
	public boolean isEmpty()
	{
		return storedMessagesCount == 0;
	}
	
	/**
	 * Indicates if the batch can hold more messages
	 * @return true is batch capacity is not reached yet, else the batch cannot be used to store more messages and must be flushed to Cradle
	 */
	public boolean isFull()
	{
		return storedMessagesCount >= messages.length;
	}
	
	/**
	 * @return summary of messages direction stored in the batch
	 */
	public Direction getMessagesDirections()
	{
		return messagesDirections;
	}
	
	/**
	 * @return mapping between streams and messages stored in the batch
	 */
	public Map<String, Set<StoredMessageId>> getStreamsMessages()
	{
		return Collections.unmodifiableMap(streamsMessages);
	}
	
	
	private void setDirection(StoredMessage message)
	{
		if (messagesDirections == null)
			messagesDirections = message.getDirection();
		else if (messagesDirections != Direction.BOTH && messagesDirections != message.getDirection())
			messagesDirections = Direction.BOTH;
	}
	
	private void mapToStream(StoredMessage message)
	{
		Set<StoredMessageId> sm = streamsMessages.get(message.getStreamName());
		if (sm == null)
		{
			sm = new HashSet<>();
			streamsMessages.put(message.getStreamName(), sm);
		}
		sm.add(message.getId());
	}
}
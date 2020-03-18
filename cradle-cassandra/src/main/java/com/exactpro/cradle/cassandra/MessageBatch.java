/******************************************************************************
 * Copyright (c) 2009-2020, Exactpro Systems LLC
 * www.exactpro.com
 * Build Software to Test Software
 *
 * All rights reserved.
 * This is unpublished, licensed software, confidential and proprietary 
 * information which is the property of Exactpro Systems LLC or its licensors.
 ******************************************************************************/

package com.exactpro.cradle.cassandra;

import java.time.Instant;
import java.util.*;

import com.datastax.oss.driver.api.core.uuid.Uuids;
import com.exactpro.cradle.Direction;
import com.exactpro.cradle.StoredMessage;
import com.exactpro.cradle.StoredMessageId;

public class MessageBatch
{
	private UUID batchId;
	private int storedMessagesCount = 0;
	private final StoredMessage[] messages;
	private final int limit;
	private Direction messagesDirections;
	private Map<String, Set<StoredMessageId>> streamsMessages;

	public MessageBatch(int limit)
	{
		this.limit = limit;
		this.messages = new StoredMessage[limit];
		this.streamsMessages = new HashMap<>();
	}

	public void init()
	{
		this.batchId = Uuids.timeBased();
		this.storedMessagesCount = 0;
	}

	public UUID getBatchId()
	{
		return batchId;
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

	public void addMessage(StoredMessage msg)
	{
		messages[storedMessagesCount++] = msg;
		setDirection(msg);
		
		Set<StoredMessageId> sm = streamsMessages.get(msg.getStreamName());
		if (sm == null)
		{
			sm = new HashSet<>();
			streamsMessages.put(msg.getStreamName(), sm);
		}
		sm.add(msg.getId());
	}

	public Instant getTimestamp()
	{
		if (messages != null && storedMessagesCount > 0)
			return messages[0].getTimestamp();
		return null;
	}

	public void clear()
	{
		batchId = null;
		storedMessagesCount = 0;
		Arrays.fill(messages, null);
		messagesDirections = null;
		streamsMessages.clear();
	}

	public boolean isEmpty()
	{
		return storedMessagesCount == 0;
	}

	public boolean isFull()
	{
		return storedMessagesCount >= limit;
	}

	private void setDirection(StoredMessage message)
	{
		if (messagesDirections == null)
			messagesDirections = message.getDirection();
		else if (messagesDirections != Direction.BOTH && messagesDirections != message.getDirection())
			messagesDirections = Direction.BOTH;
	}

	public Direction getMessagesDirections()
	{
		return messagesDirections;
	}

	public Map<String, Set<StoredMessageId>> getStreamsMessages()
	{
		return streamsMessages;
	}
}

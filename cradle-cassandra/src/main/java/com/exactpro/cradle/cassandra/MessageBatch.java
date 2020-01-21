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

import com.exactpro.cradle.Direction;
import com.exactpro.cradle.StoredMessage;

public class MessageBatch
{
	private UUID batchId;
	private int storedMessagesCount = 0;
	private final StoredMessage[] messages;
	private final int limit;
	private Direction msgsDirections;
	private Set<String> msgsStreamsNames;

	public MessageBatch(int limit)
	{
		this.limit = limit;
		this.messages = new StoredMessage[limit];
		this.msgsStreamsNames = new HashSet<>();
	}

	public void init()
	{
		this.batchId = UUID.randomUUID();
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

	public void addMessage(StoredMessage msg)
	{
		messages[storedMessagesCount++] = msg;
		setDirection(msg);
		msgsStreamsNames.add(msg.getStreamName());
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
		msgsDirections = null;
		msgsStreamsNames.clear();
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
		if (msgsDirections == null)
		{
			msgsDirections = message.getDirection();
		}
		else if (msgsDirections != Direction.BOTH && msgsDirections != message.getDirection())
		{
			msgsDirections = Direction.BOTH;
		}
	}

	public Direction getMsgsDirections()
	{
		return msgsDirections;
	}

	public Set<String> getMsgsStreamsNames()
	{
		return msgsStreamsNames;
	}
}

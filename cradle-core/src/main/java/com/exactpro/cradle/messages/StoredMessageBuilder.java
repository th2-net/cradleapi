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

import com.exactpro.cradle.Direction;

public class StoredMessageBuilder
{
	private StoredMessage msg;
	
	public StoredMessageBuilder()
	{
		msg = createStoredMessage();
	}
	
	
	protected StoredMessage createStoredMessage()
	{
		return new StoredMessage();
	}
	
	private void initIfNeeded()
	{
		if (msg == null)
			msg = createStoredMessage();
	}
	
	
	public StoredMessageBuilder id(StoredMessageId id)
	{
		initIfNeeded();
		msg.setId(id);
		return this;
	}
	
	public StoredMessageBuilder content(byte[] message)
	{
		initIfNeeded();
		msg.setContent(message);
		return this;
	}
	
	public StoredMessageBuilder sent()
	{
		initIfNeeded();
		msg.setDirection(Direction.SENT);
		return this;
	}
	
	public StoredMessageBuilder received()
	{
		initIfNeeded();
		msg.setDirection(Direction.RECEIVED);
		return this;
	}
	
	public StoredMessageBuilder direction(Direction d)
	{
		initIfNeeded();
		msg.setDirection(d);
		return this;
	}
	
	public StoredMessageBuilder timestamp(Instant timestamp)
	{
		initIfNeeded();
		msg.setTimestamp(timestamp);
		return this;
	}
	
	public StoredMessageBuilder streamName(String streamName)
	{
		initIfNeeded();
		msg.setStreamName(streamName);
		return this;
	}
	
	public StoredMessage build()
	{
		initIfNeeded();
		StoredMessage result = msg;
		msg = null;
		return result;
	}
}
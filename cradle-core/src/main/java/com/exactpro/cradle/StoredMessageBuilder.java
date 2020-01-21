/******************************************************************************
 * Copyright (c) 2009-2020, Exactpro Systems LLC
 * www.exactpro.com
 * Build Software to Test Software
 *
 * All rights reserved.
 * This is unpublished, licensed software, confidential and proprietary 
 * information which is the property of Exactpro Systems LLC or its licensors.
 ******************************************************************************/

package com.exactpro.cradle;

import java.time.Instant;

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
	
	
	public StoredMessageBuilder id(StoredMessageId id)
	{
		msg.setId(id);
		return this;
	}
	
	public StoredMessageBuilder message(byte[] message)
	{
		msg.setMessage(message);
		return this;
	}
	
	public StoredMessageBuilder sent()
	{
		msg.setDirection(Direction.SENT);
		return this;
	}
	
	public StoredMessageBuilder received()
	{
		msg.setDirection(Direction.RECEIVED);
		return this;
	}
	
	public StoredMessageBuilder direction(Direction d)
	{
		msg.setDirection(d);
		return this;
	}
	
	public StoredMessageBuilder timestamp(Instant timestamp)
	{
		msg.setTimestamp(timestamp);
		return this;
	}
	
	public StoredMessageBuilder streamName(String streamName)
	{
		msg.setStreamName(streamName);
		return this;
	}
	
	public StoredMessage build()
	{
		StoredMessage result = msg;
		msg = createStoredMessage();
		return result;
	}
}
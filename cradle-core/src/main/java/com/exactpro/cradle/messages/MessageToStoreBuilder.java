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

/**
 * Builder for MessageToStore object. After calling {@link #build()} method, the builder can be reused to build new message
 */
public class MessageToStoreBuilder
{
	private MessageToStore msg;
	
	public MessageToStoreBuilder()
	{
		msg = createMessageToStore();
	}
	
	
	protected MessageToStore createMessageToStore()
	{
		return new MessageToStore();
	}
	
	private void initIfNeeded()
	{
		if (msg == null)
			msg = createMessageToStore();
	}
	
	
	public MessageToStoreBuilder streamName(String streamName)
	{
		initIfNeeded();
		msg.setStreamName(streamName);
		return this;
	}
	
	public MessageToStoreBuilder direction(Direction d)
	{
		initIfNeeded();
		msg.setDirection(d);
		return this;
	}
	
	public MessageToStoreBuilder index(long index)
	{
		initIfNeeded();
		msg.setIndex(index);
		return this;
	}
	
	public MessageToStoreBuilder timestamp(Instant timestamp)
	{
		initIfNeeded();
		msg.setTimestamp(timestamp);
		return this;
	}
	
	public MessageToStoreBuilder content(byte[] content)
	{
		initIfNeeded();
		msg.setContent(content);
		return this;
	}
	
	
	public MessageToStore build()
	{
		initIfNeeded();
		MessageToStore result = msg;
		msg = null;
		return result;
	}
}
/******************************************************************************
 * Copyright (c) 2009-2020, Exactpro Systems LLC
 * www.exactpro.com
 * Build Software to Test Software
 *
 * All rights reserved.
 * This is unpublished, licensed software, confidential and proprietary 
 * information which is the property of Exactpro Systems LLC or its licensors.
 ******************************************************************************/

package com.exactpro.cradle.feeder.messages;

import java.nio.charset.StandardCharsets;
import java.time.Instant;

import com.exactpro.cradle.Direction;
import com.exactpro.cradle.messages.MessageToStore;
import com.exactpro.cradle.messages.MessageToStoreBuilder;
import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class JsonStoredMessage
{
	private String message, 
			direction,
			streamName,
			timestamp,
			eventId;

	public String getMessage()
	{
		return message;
	}
	
	public void setMessage(String message)
	{
		this.message = message;
	}
	
	
	public String getDirection()
	{
		return direction;
	}
	
	public void setDirection(String direction)
	{
		this.direction = direction;
	}
	
	
	public String getStreamName()
	{
		return streamName;
	}
	
	public void setStreamName(String streamName)
	{
		this.streamName = streamName;
	}
	
	
	public String getTimestamp()
	{
		return timestamp;
	}
	
	public void setTimestamp(String timestamp)
	{
		this.timestamp = timestamp;
	}
	
	
	public String getEventId()
	{
		return eventId;
	}
	
	public void setEventId(String eventId)
	{
		this.eventId = eventId;
	}
	
	
	public MessageToStore toMessage(long index)
	{
		return new MessageToStoreBuilder()
				.streamName(streamName)
				.direction(Direction.byLabel(direction))
				.index(index)
				.timestamp(Instant.parse(timestamp))
				.content(message.getBytes(StandardCharsets.UTF_8))
				.build();
	}
}
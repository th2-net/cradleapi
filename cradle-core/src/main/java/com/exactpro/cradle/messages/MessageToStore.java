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
import java.util.Arrays;

import com.exactpro.cradle.Direction;
import com.exactpro.cradle.utils.CompressionUtils;

/**
 * Object to hold information about one message prepared to be stored in Cradle
 */
public class MessageToStore
{
	private String streamName;
	private Direction direction;
	private long index;
	private Instant timestamp;
	private MessageMetadata metadata = null;
	private byte[] content;
	
	public MessageToStore()
	{
		index = -1;
	}
	
	public MessageToStore(MessageToStore copyFrom)
	{
		this.streamName = copyFrom.getStreamName();
		this.direction = copyFrom.getDirection();
		this.index = copyFrom.getIndex();
		this.timestamp = copyFrom.getTimestamp();
		this.metadata = copyFrom.getMetadata() != null ? new MessageMetadata(copyFrom.getMetadata()) : null;
		this.content = copyFrom.getContent();
	}
	
	
	/**
	 * @return name of stream the message is related to
	 */
	public String getStreamName()
	{
		return streamName;
	}
	
	public void setStreamName(String streamName)
	{
		this.streamName = streamName;
	}
	
	
	/**
	 * @return direction in which the message went through the stream
	 */
	public Direction getDirection()
	{
		return direction;
	}
	
	public void setDirection(Direction direction)
	{
		this.direction = direction;
	}
	
	
	/**
	 * @return index the message has for its stream and direction
	 */
	public long getIndex()
	{
		return index;
	}
	
	public void setIndex(long index)
	{
		this.index = index;
	}
	
	
	/**
	 * @return timestamp of message creation
	 */
	public Instant getTimestamp()
	{
		return timestamp;
	}
	
	public void setTimestamp(Instant timestamp)
	{
		this.timestamp = timestamp;
	}
	
	
	public MessageMetadata getMetadata()
	{
		return metadata;
	}
	
	public void setMetadata(MessageMetadata metadata)
	{
		this.metadata = metadata;
	}
	
	public void addMetadata(String key, String value)
	{
		if (metadata == null)
			metadata = new MessageMetadata();
		metadata.add(key, value);
	}
	
	
	/**
	 * @return message content
	 */
	public byte[] getContent()
	{
		return content;
	}
	
	public void setContent(byte[] message)
	{
		this.content = message;
	}
	
	
	@Override
	public String toString()
	{
		return new StringBuilder()
				.append("MessageToStore{").append(CompressionUtils.EOL)
				.append("streamName=").append(streamName).append(",").append(CompressionUtils.EOL)
				.append("direction=").append(direction).append(",").append(CompressionUtils.EOL)
				.append("index=").append(index).append(",").append(CompressionUtils.EOL)
				.append("timestamp=").append(timestamp).append(",").append(CompressionUtils.EOL)
				.append("metadata=").append(metadata).append(",").append(CompressionUtils.EOL)
				.append("content=").append(Arrays.toString(content)).append(CompressionUtils.EOL)
				.append("}").toString();
	}
}
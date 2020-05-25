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

import java.io.Serializable;
import java.time.Instant;
import java.util.Arrays;

import com.exactpro.cradle.Direction;
import com.exactpro.cradle.utils.CompressionUtils;

/**
 * Holds information about one message stored in Cradle.
 */
public class StoredMessage implements Serializable
{
	private static final long serialVersionUID = 200983136307497672L;
	
	private final StoredMessageId id;
	private final Instant timestamp;
	private final byte[] content;
	
	public StoredMessage(MessageToStore message, StoredMessageId id)
	{
		this.id = id;
		this.timestamp = message.getTimestamp();
		this.content = message.getContent();
	}
	
	public StoredMessage(StoredMessage copyFrom, StoredMessageId id)
	{
		this.id = id;
		this.timestamp = copyFrom.getTimestamp();
		this.content = copyFrom.getContent();
	}
	
	public StoredMessage(StoredMessage copyFrom)
	{
		this(copyFrom, copyFrom.getId());
	}
	
	
	/**
	 * @return unique message ID as stored in Cradle.
	 * Result of this method should be used for referencing stored messages to obtain them from Cradle
	 */
	public StoredMessageId getId()
	{
		return id;
	}
	
	/**
	 * @return name of stream the message is related to
	 */
	public String getStreamName()
	{
		return id.getStreamName();
	}
	
	/**
	 * @return direction in which the message went through the stream
	 */
	public Direction getDirection()
	{
		return id.getDirection();
	}
	
	/**
	 * @return index the message has for its stream and direction
	 */
	public long getIndex()
	{
		return id.getIndex();
	}
	
	/**
	 * @return timestamp of message creation
	 */
	public Instant getTimestamp()
	{
		return timestamp;
	}
	
	/**
	 * @return message content
	 */
	public byte[] getContent()
	{
		return content;
	}
	
	@Override
	public String toString()
	{
		return new StringBuilder()
				.append("StoredMessage{").append(CompressionUtils.EOL)
				.append("ID=").append(id).append(",").append(CompressionUtils.EOL)
				.append("streamName=").append(getStreamName()).append(",").append(CompressionUtils.EOL)
				.append("direction=").append(getDirection()).append(",").append(CompressionUtils.EOL)
				.append("index=").append(getIndex()).append(",").append(CompressionUtils.EOL)
				.append("timestamp=").append(getTimestamp()).append(",").append(CompressionUtils.EOL)
				.append("content=").append(Arrays.toString(getContent())).append(CompressionUtils.EOL)
				.append("}").toString();
	}
}
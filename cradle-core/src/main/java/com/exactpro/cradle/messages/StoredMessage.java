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
	private final StoredMessageMetadata metadata;
	private final byte[] content;
	
	public StoredMessage(MessageToStore message, StoredMessageId id)
	{
		this.id = id;
		this.timestamp = message.getTimestamp();
		this.metadata = message.getMetadata() != null ? new StoredMessageMetadata(message.getMetadata()) : null;
		this.content = message.getContent();
	}
	
	public StoredMessage(StoredMessage copyFrom, StoredMessageId id)
	{
		this.id = id;
		this.timestamp = copyFrom.getTimestamp();
		this.metadata = copyFrom.getMetadata() != null ? new StoredMessageMetadata(copyFrom.getMetadata()) : null;
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
	 * @return metadata attached to message
	 */
	public StoredMessageMetadata getMetadata()
	{
		return metadata;
	}
	
	/**
	 * @return message content
	 */
	public byte[] getContent()
	{
		return content;
	}
	
	
	@Override
	public int hashCode()
	{
		final int prime = 31;
		int result = 1;
		result = prime * result + Arrays.hashCode(content);
		result = prime * result + ((id == null) ? 0 : id.hashCode());
		result = prime * result + ((metadata == null) ? 0 : metadata.hashCode());
		result = prime * result + ((timestamp == null) ? 0 : timestamp.hashCode());
		return result;
	}
	
	@Override
	public boolean equals(Object obj)
	{
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		StoredMessage other = (StoredMessage) obj;
		if (!Arrays.equals(content, other.content))
			return false;
		if (id == null)
		{
			if (other.id != null)
				return false;
		} else if (!id.equals(other.id))
			return false;
		if (metadata == null)
		{
			if (other.metadata != null)
				return false;
		} else if (!metadata.equals(other.metadata))
			return false;
		if (timestamp == null)
		{
			if (other.timestamp != null)
				return false;
		} else if (!timestamp.equals(other.timestamp))
			return false;
		return true;
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
				.append("metadata=").append(getMetadata()).append(",").append(CompressionUtils.EOL)
				.append("content=").append(Arrays.toString(getContent())).append(CompressionUtils.EOL)
				.append("}").toString();
	}
}
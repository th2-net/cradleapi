/*
 * Copyright 2020-2021 Exactpro (Exactpro Systems Limited)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.exactpro.cradle.messages;

import java.io.Serializable;
import java.time.Instant;
import java.util.Arrays;

import com.exactpro.cradle.BookId;
import com.exactpro.cradle.Direction;
import com.exactpro.cradle.utils.CompressionUtils;

/**
 * Holds information about one message stored in Cradle.
 */
public class StoredMessage implements Serializable
{
	private static final long serialVersionUID = 5602557739148866986L;
	
	private final StoredMessageId id;
	private final StoredMessageMetadata metadata;
	private final byte[] content;
	
	public StoredMessage(MessageToStore message, StoredMessageId id)
	{
		this.id = id;
		this.metadata = message.getMetadata() != null ? new StoredMessageMetadata(message.getMetadata()) : null;
		this.content = message.getContent();
	}
	
	public StoredMessage(StoredMessage copyFrom, StoredMessageId id)
	{
		this.id = id;
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
	 * @return ID of book the message is related to
	 */
	public BookId getBookId()
	{
		return id.getBookId();
	}
	
	/**
	 * @return alias of session the message is related to
	 */
	public String getSessionAlias()
	{
		return id.getSessionAlias();
	}
	
	/**
	 * @return direction in which the message went through the stream
	 */
	public Direction getDirection()
	{
		return id.getDirection();
	}
	
	/**
	 * @return timestamp of message creation
	 */
	public Instant getTimestamp()
	{
		return id.getTimestamp();
	}
	
	/**
	 * @return sequence number the message has for its session, direction and timestamp
	 */
	public long getSequence()
	{
		return id.getSequence();
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
		return true;
	}
	
	@Override
	public String toString()
	{
		return new StringBuilder()
				.append("StoredMessage{").append(CompressionUtils.EOL)
				.append("ID=").append(id).append(",").append(CompressionUtils.EOL)
				.append("metadata=").append(getMetadata()).append(",").append(CompressionUtils.EOL)
				.append("content=").append(Arrays.toString(getContent())).append(CompressionUtils.EOL)
				.append("}").toString();
	}
}

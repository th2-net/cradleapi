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
import com.exactpro.cradle.PageId;
import com.exactpro.cradle.utils.CompressionUtils;

/**
 * Holds information about one message stored in Cradle.
 */
public class StoredMessage implements Serializable, CradleMessage
{
	private static final long serialVersionUID = 5602557739148866986L;
	
	private final StoredMessageId id;
	private final byte[] content;
	private final StoredMessageMetadata metadata;
	private final PageId pageId;
	
	public StoredMessage(CradleMessage message, StoredMessageId id, PageId pageId)
	{
		this.id = id;
		this.content = message.getContent();
		this.metadata = message.getMetadata() != null ? new StoredMessageMetadata(message.getMetadata()) : null;
		this.pageId = pageId;
	}
	
	public StoredMessage(StoredMessage copyFrom)
	{
		this(copyFrom, copyFrom.getId(), copyFrom.getPageId());
	}
	
	
	/**
	 * @return unique message ID as stored in Cradle.
	 * Result of this method should be used for referencing stored messages to obtain them from Cradle
	 */
	public StoredMessageId getId()
	{
		return id;
	}

	@Override
	public BookId getBookId()
	{
		return id.getBookId();
	}

	@Override
	public String getSessionAlias()
	{
		return id.getSessionAlias();
	}

	@Override
	public Direction getDirection()
	{
		return id.getDirection();
	}

	@Override
	public Instant getTimestamp()
	{
		return id.getTimestamp();
	}

	@Override
	public long getSequence()
	{
		return id.getSequence();
	}

	@Override
	public byte[] getContent()
	{
		return content;
	}
	
	@Override
	public StoredMessageMetadata getMetadata()
	{
		return metadata;
	}

	public PageId getPageId()
	{
		return pageId;
	}
	
	
	@Override
	public int hashCode()
	{
		final int prime = 31;
		int result = 1;
		result = prime * result + Arrays.hashCode(content);
		result = prime * result + ((id == null) ? 0 : id.hashCode());
		result = prime * result + ((metadata == null) ? 0 : metadata.hashCode());
		result = prime * result + ((pageId == null) ? 0 : pageId.hashCode());
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
		
		if (pageId == null)
		{
			if (other.pageId != null)
				return false;
		} else if (!pageId.equals(other.pageId))
			return false;
		
		return true;
	}
	
	@Override
	public String toString()
	{
		return new StringBuilder()
				.append("StoredMessage{").append(CompressionUtils.EOL)
				.append("id=").append(id).append(",").append(CompressionUtils.EOL)
				.append("content=").append(Arrays.toString(getContent())).append(CompressionUtils.EOL)
				.append("metadata=").append(getMetadata()).append(",").append(CompressionUtils.EOL)
				.append("pageId=").append(getPageId()).append(CompressionUtils.EOL)
				.append("}").toString();
	}
}

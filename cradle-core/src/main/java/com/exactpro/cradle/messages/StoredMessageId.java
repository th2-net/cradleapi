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

import com.exactpro.cradle.Direction;
import com.exactpro.cradle.exceptions.CradleIdException;

/**
 * Holds ID of a message stored in Cradle.
 * All messages have sequenced index, scoped by direction and stream related to the message.
 * Message index in conjunction with stream name and direction of the message form the message ID
 */
public class StoredMessageId implements Serializable
{
	private static final long serialVersionUID = -6856521491563727644L;
	
	private final String streamName;
	private final Direction direction;
	private final long index;
	
	public StoredMessageId(String streamName, Direction direction, long index)
	{
		this.streamName = streamName;
		this.direction = direction;
		this.index = index;
	}
	
	
	public static StoredMessageId fromString(String id) throws CradleIdException
	{
		String[] parts = StoredMessageIdUtils.splitParts(id);
		if (parts.length < 3)
			throw new CradleIdException("Message ID ("+id+") should contain stream name, direction and message index delimited with '"+StoredMessageBatchId.IDS_DELIMITER+"'");
		
		long index = StoredMessageIdUtils.getIndex(parts);
		Direction direction = StoredMessageIdUtils.getDirection(parts);
		String streamName = StoredMessageIdUtils.getStreamName(parts);
		return new StoredMessageId(streamName, direction, index);
	}
	
	
	public String getStreamName()
	{
		return streamName;
	}
	
	public Direction getDirection()
	{
		return direction;
	}
	
	public long getIndex()
	{
		return index;
	}
	
	
	@Override
	public String toString()
	{
		return streamName+StoredMessageBatchId.IDS_DELIMITER+direction.getLabel()+StoredMessageBatchId.IDS_DELIMITER+index;
	}
	
	@Override
	public int hashCode()
	{
		final int prime = 31;
		int result = 1;
		result = prime * result + ((direction == null) ? 0 : direction.hashCode());
		result = prime * result + (int) (index ^ (index >>> 32));
		result = prime * result + ((streamName == null) ? 0 : streamName.hashCode());
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
		StoredMessageId other = (StoredMessageId) obj;
		if (direction != other.direction)
			return false;
		if (index != other.index)
			return false;
		if (streamName == null)
		{
			if (other.streamName != null)
				return false;
		} else if (!streamName.equals(other.streamName))
			return false;
		return true;
	}
}

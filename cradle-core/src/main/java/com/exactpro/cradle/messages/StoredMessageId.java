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

import com.exactpro.cradle.Direction;
import com.exactpro.cradle.utils.CradleIdException;

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
		String[] parts = id.split(StoredMessageBatchId.IDS_DELIMITER);
		if (parts.length < 3)
			throw new CradleIdException("Message ID ("+id+") should contain stream name, direction and message index delimited with '"+StoredMessageBatchId.IDS_DELIMITER+"'");
		
		long index;
		try
		{
			index = Long.parseLong(parts[2]);
		}
		catch (NumberFormatException e)
		{
			throw new CradleIdException("Invalid message index ("+parts[2]+") in ID '"+id+"'");
		}
		
		Direction direction = Direction.byLabel(parts[1]);
		if (direction == null)
			throw new CradleIdException("Invalid direction '"+parts[1]+"'");
		
		return new StoredMessageId(parts[0], direction, index);
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
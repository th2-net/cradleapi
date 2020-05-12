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
 * Holds ID of messages batch stored in Cradle.
 * All messages have sequenced index, scoped by direction and stream related to the message.
 * Messages in the batch are supposed to be sequenced. ID contains index of first message within the batch
 */
public class StoredMessageBatchId implements Serializable
{
	private static final long serialVersionUID = 4457250386017662885L;
	public static final String IDS_DELIMITER = ":";
	
	private final String streamName;
	private final Direction direction;
	private final long index;
	
	public StoredMessageBatchId(String streamName, Direction direction, long index)
	{
		this.streamName = streamName;
		this.direction = direction;
		this.index = index;
	}
	
	
	public static StoredMessageBatchId fromString(String id) throws CradleIdException
	{
		String[] parts = id.split(IDS_DELIMITER);
		if (parts.length < 3)
			throw new CradleIdException("Batch ID ("+id+") should contain stream name, direction and message index delimited with '"+IDS_DELIMITER+"'");
		
		int index;
		try
		{
			index = Integer.parseInt(parts[2]);
		}
		catch (NumberFormatException e)
		{
			throw new CradleIdException("Invalid message index ("+parts[2]+") in batch ID '"+id+"'");
		}
		
		Direction direction = Direction.byLabel(parts[1]);
		if (direction == null)
			throw new CradleIdException("Invalid direction '"+parts[1]+"'");
		
		return new StoredMessageBatchId(parts[0], direction, index);
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
		return streamName+IDS_DELIMITER+direction.getLabel()+IDS_DELIMITER+index;
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
		
		StoredMessageBatchId other = (StoredMessageBatchId) obj;
		if (direction == null)
		{
			if (other.direction != null)
				return false;
		}
		else if (!direction.equals(other.direction))
			return false;
		
		if (index != other.index)
			return false;
		
		if (streamName == null)
		{
			if (other.streamName != null)
				return false;
		}
		else if (!streamName.equals(other.streamName))
			return false;
		
		return true;
	}
}
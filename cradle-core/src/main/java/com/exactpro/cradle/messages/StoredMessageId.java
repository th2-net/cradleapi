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

import com.exactpro.cradle.utils.CradleIdException;

/**
 * Holds ID of a message stored in Cradle.
 * All messages have sequenced index, scoped by direction and stream related to the message.
 * Message index in conjunction with ID of batch the message is stored in, form the message ID
 */
public class StoredMessageId implements Serializable
{
	private static final long serialVersionUID = -6856521491563727644L;
	
	private final StoredMessageBatchId batchId;
	private final long index;
	
	public StoredMessageId(StoredMessageBatchId batchId, long index)
	{
		this.batchId = batchId;
		this.index = index;
	}
	
	
	public static StoredMessageId fromString(String id) throws CradleIdException
	{
		StoredMessageBatchId batchId = StoredMessageBatchId.fromString(id);
		
		String indexPart = id.substring(batchId.toString().length()+StoredMessageBatchId.IDS_DELIMITER.length());
		int index;
		try
		{
			index = Integer.parseInt(indexPart);
		}
		catch (NumberFormatException e)
		{
			throw new CradleIdException("Invalid message index ("+indexPart+") in message ID '"+id+"'");
		}
		
		return new StoredMessageId(batchId, index);
	}
	
	
	public StoredMessageBatchId getBatchId()
	{
		return batchId;
	}
	
	public long getIndex()
	{
		return index;
	}
	
	
	@Override
	public String toString()
	{
		return batchId.toString()+StoredMessageBatchId.IDS_DELIMITER+index;
	}
	
	@Override
	public int hashCode()
	{
		final int prime = 31;
		int result = 1;
		result = prime * result + ((batchId == null) ? 0 : batchId.hashCode());
		result = prime * result + (int) (index ^ (index >>> 32));
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
		if (batchId == null)
		{
			if (other.batchId != null)
				return false;
		} else if (!batchId.equals(other.batchId))
			return false;
		if (index != other.index)
			return false;
		return true;
	}
}
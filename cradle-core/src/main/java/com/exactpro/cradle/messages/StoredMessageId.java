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

/**
 * Holds ID of a message stored in Cradle.
 * All messages are supposed to be stored in batches, so message ID contains ID of batch the message is stored in
 */
public class StoredMessageId implements Serializable
{
	private static final long serialVersionUID = 7369523107026579370L;
	public static final String IDS_DELIMITER = ":";
	
	private final StoredMessageBatchId batchId;
	private final int index;
	
	public StoredMessageId(StoredMessageBatchId batchId, int index)
	{
		this.batchId = batchId;
		this.index = index;
	}
	
	
	public StoredMessageBatchId getBatchId()
	{
		return batchId;
	}
	
	public int getIndex()
	{
		return index;
	}
	
	
	@Override
	public String toString()
	{
		return batchId.toString()+":"+index;
	}
	
	@Override
	public int hashCode()
	{
		final int prime = 31;
		int result = 1;
		result = prime * result + ((batchId == null) ? 0 : batchId.hashCode());
		result = prime * result + index;
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
		if (batchId == null) {
			if (other.batchId != null)
				return false;
		} else if (!batchId.equals(other.batchId))
			return false;
		if (index != other.index)
			return false;
		return true;
	}
}
/******************************************************************************
 * Copyright (c) 2009-2020, Exactpro Systems LLC
 * www.exactpro.com
 * Build Software to Test Software
 *
 * All rights reserved.
 * This is unpublished, licensed software, confidential and proprietary 
 * information which is the property of Exactpro Systems LLC or its licensors.
 ******************************************************************************/

package com.exactpro.cradle.testevents;

import java.io.Serializable;

import com.exactpro.cradle.utils.CradleIdException;

/**
 * Holds ID of a test event stored in Cradle
 * All test events are supposed to be stored in batches, so test event ID contains ID of batch the event is stored in
 */
public class StoredTestEventId implements Serializable
{
	private static final long serialVersionUID = -2617119276298695631L;

	public static final String IDS_DELIMITER = ":";
	
	private final StoredTestEventBatchId batchId;
	private final int index;
	
	public StoredTestEventId(StoredTestEventBatchId batchId, int index)
	{
		this.batchId = batchId;
		this.index = index;
	}
	
	
	public static StoredTestEventId fromString(String id) throws CradleIdException
	{
		String[] parts = id.split(IDS_DELIMITER);
		if (parts.length < 2)
			throw new CradleIdException("Test event ID ("+id+") should contain batch ID and test event index delimited with '"+IDS_DELIMITER+"'");
		
		int index;
		try
		{
			index = Integer.parseInt(parts[1]);
		}
		catch (NumberFormatException e)
		{
			throw new CradleIdException("Invalid test event index ("+parts[1]+") in test event ID '"+id+"'");
		}
		
		return new StoredTestEventId(new StoredTestEventBatchId(parts[0]), index);
	}
	
	
	public StoredTestEventBatchId getBatchId()
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
		return batchId.toString()+IDS_DELIMITER+index;
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
		StoredTestEventId other = (StoredTestEventId) obj;
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
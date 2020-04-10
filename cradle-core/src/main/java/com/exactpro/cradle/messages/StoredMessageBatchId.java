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
 * Holds ID of message batch. It will be used in StoredMessageId of messages stored in corresponding batch
 */
public class StoredMessageBatchId implements Serializable
{
	private static final long serialVersionUID = 3345202917184581650L;
	
	private final String id;
	private final int hashcode;
	
	public StoredMessageBatchId(String id)
	{
		this.id = id;
		this.hashcode = calcHashcode();
	}
	
	
	public String getId()
	{
		return id;
	}
	
	
	@Override
	public String toString()
	{
		return id;
	}

	@Override
	public int hashCode()
	{
		return hashcode;
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
		if (id == null) {
			if (other.id != null)
				return false;
		} else if (!id.equals(other.id))
			return false;
		return true;
	}
	
	
	protected int calcHashcode()
	{
		final int prime = 31;
		int result = 1;
		result = prime * result + ((id == null) ? 0 : id.hashCode());
		return result;
	}
}
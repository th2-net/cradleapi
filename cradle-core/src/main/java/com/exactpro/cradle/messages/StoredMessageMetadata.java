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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Unmodifiable metadata of message stored in Cradle
 */
public class StoredMessageMetadata implements Serializable
{
	private static final long serialVersionUID = -3397537024211430427L;
	
	protected final Map<String, String> data = new HashMap<>();
	
	public StoredMessageMetadata()
	{
	}
	
	public StoredMessageMetadata(StoredMessageMetadata metadata)
	{
		this.data.putAll(metadata.data);
	}
	
	
	@Override
	public String toString()
	{
		return data.toString();
	}
	
	
	public String get(String key)
	{
		return data.get(key);
	}
	
	public Set<String> getKeys()
	{
		return Collections.unmodifiableSet(data.keySet());
	}
	
	public Map<String, String> toMap()
	{
		return Collections.unmodifiableMap(data);
	}
	
	
	@Override
	public int hashCode()
	{
		final int prime = 31;
		int result = 1;
		result = prime * result + ((data == null) ? 0 : data.hashCode());
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
		StoredMessageMetadata other = (StoredMessageMetadata) obj;
		if (data == null)
		{
			if (other.data != null)
				return false;
		} else if (!data.equals(other.data))
			return false;
		return true;
	}
}

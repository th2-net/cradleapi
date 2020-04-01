/******************************************************************************
 * Copyright (c) 2009-2020, Exactpro Systems LLC
 * www.exactpro.com
 * Build Software to Test Software
 *
 * All rights reserved.
 * This is unpublished, licensed software, confidential and proprietary 
 * information which is the property of Exactpro Systems LLC or its licensors.
 ******************************************************************************/

package com.exactpro.cradle;

import java.io.Serializable;

/**
 * Holder for data to find message stored in CradleStorage.
 * Extend this class with additional fields/methods
 */
public class StoredMessageId implements Serializable
{
	private static final long serialVersionUID = 7369523107026579370L;
	
	private String id;
	
	public StoredMessageId() {}

	public StoredMessageId(String id)
	{
		this.id = id;
	}

	public String getId()
	{
		return id;
	}
	
	public void setId(String id)
	{
		this.id = id;
	}

	@Override
	public String toString()
	{
		return id;
	}
}

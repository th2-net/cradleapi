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

import com.exactpro.cradle.utils.CradleStorageException;

public class MinimalStoredTestEvent implements MinimalTestEventFields
{
	private final StoredTestEventId id;
	private final String name,
			type;
	private final StoredTestEventId parentId;
	
	public MinimalStoredTestEvent(MinimalTestEventFields event) throws CradleStorageException
	{
		this.id = event.getId();
		this.name = event.getName();
		this.type = event.getType();
		this.parentId = event.getParentId();
		
		if (this.id == null)
			throw new CradleStorageException("Test event ID cannot be null");
		if (this.id.equals(parentId))
			throw new CradleStorageException("Test event cannot reference itself");
	}
	
	
	@Override
	public StoredTestEventId getId()
	{
		return id;
	}
	
	@Override
	public String getName()
	{
		return name;
	}
	
	@Override
	public String getType()
	{
		return type;
	}
	
	@Override
	public StoredTestEventId getParentId()
	{
		return parentId;
	}
}

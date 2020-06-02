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
	
	public MinimalStoredTestEvent(StoredTestEventId id, String name, String type, StoredTestEventId parentId) throws CradleStorageException
	{
		this.id = id;
		this.name = name;
		this.type = type;
		this.parentId = parentId;
		
		if (this.id == null)
			throw new CradleStorageException("Test event ID cannot be null");
		if (this.id.equals(parentId))
			throw new CradleStorageException("Test event cannot reference itself");
	}
	
	public MinimalStoredTestEvent(MinimalTestEventFields event) throws CradleStorageException
	{
		this(event.getId(), event.getName(), event.getType(), event.getParentId());
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

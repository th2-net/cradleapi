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

/**
 * Builder for MinimalTestEventToStore object. After calling {@link #build()} method, the builder can be reused to build new test event
 */
public class MinimalTestEventToStoreBuilder
{
	private MinimalTestEventToStore event;
	
	public MinimalTestEventToStoreBuilder()
	{
		event = createTestEventToStore();
	}
	
	
	protected MinimalTestEventToStore createTestEventToStore()
	{
		return new MinimalTestEventToStore();
	}
	
	private void initIfNeeded()
	{
		if (event == null)
			event = createTestEventToStore();
	}
	
	
	public MinimalTestEventToStoreBuilder id(StoredTestEventId id)
	{
		initIfNeeded();
		event.setId(id);
		return this;
	}
	
	public MinimalTestEventToStoreBuilder name(String name)
	{
		initIfNeeded();
		event.setName(name);
		return this;
	}
	
	public MinimalTestEventToStoreBuilder type(String type)
	{
		initIfNeeded();
		event.setType(type);
		return this;
	}
	
	public MinimalTestEventToStoreBuilder parentId(StoredTestEventId parentId)
	{
		initIfNeeded();
		event.setParentId(parentId);
		return this;
	}
	
	
	public MinimalTestEventToStore build()
	{
		initIfNeeded();
		MinimalTestEventToStore result = event;
		event = null;
		return result;
	}
}
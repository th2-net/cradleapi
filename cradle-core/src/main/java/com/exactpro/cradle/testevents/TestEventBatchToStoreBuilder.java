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

import java.util.UUID;

/**
 * Builder for {@link TestEventBatchToStore} object. After calling {@link #build()} method, the builder can be reused to build new test event
 */
public class TestEventBatchToStoreBuilder
{
	private TestEventBatchToStore event;
	
	public TestEventBatchToStoreBuilder()
	{
		event = createTestEventToStore();
	}
	
	
	protected TestEventBatchToStore createTestEventToStore()
	{
		return new TestEventBatchToStore();
	}
	
	private void initIfNeeded()
	{
		if (event == null)
			event = createTestEventToStore();
	}
	
	
	public TestEventBatchToStoreBuilder id(StoredTestEventId id)
	{
		initIfNeeded();
		event.setId(id);
		return this;
	}
	
	public TestEventBatchToStoreBuilder idRandom()
	{
		initIfNeeded();
		event.setId(new StoredTestEventId(UUID.randomUUID().toString()));
		return this;
	}
	
	public TestEventBatchToStoreBuilder name(String name)
	{
		initIfNeeded();
		event.setName(name);
		return this;
	}
	
	public TestEventBatchToStoreBuilder type(String type)
	{
		initIfNeeded();
		event.setType(type);
		return this;
	}
	
	public TestEventBatchToStoreBuilder parentId(StoredTestEventId parentId)
	{
		initIfNeeded();
		event.setParentId(parentId);
		return this;
	}
	
	
	public TestEventBatchToStore build()
	{
		initIfNeeded();
		TestEventBatchToStore result = event;
		event = null;
		return result;
	}
}
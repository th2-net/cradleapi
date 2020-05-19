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

import java.time.Instant;

/**
 * Builder for TestEventToStore object. After calling {@link #build()} method, the builder can be reused to build new test event
 */
public class TestEventToStoreBuilder
{
	private TestEventToStore event;
	
	public TestEventToStoreBuilder()
	{
		event = createTestEventToStore();
	}
	
	
	protected TestEventToStore createTestEventToStore()
	{
		return new TestEventToStore();
	}
	
	private void initIfNeeded()
	{
		if (event == null)
			event = createTestEventToStore();
	}
	
	
	public TestEventToStoreBuilder id(StoredTestEventId id)
	{
		initIfNeeded();
		event.setId(id);
		return this;
	}
	
	public TestEventToStoreBuilder name(String name)
	{
		initIfNeeded();
		event.setName(name);
		return this;
	}
	
	public TestEventToStoreBuilder type(String type)
	{
		initIfNeeded();
		event.setType(type);
		return this;
	}
	
	public TestEventToStoreBuilder parentId(StoredTestEventId parentId)
	{
		initIfNeeded();
		event.setParentId(parentId);
		return this;
	}
	
	public TestEventToStoreBuilder startTimestamp(Instant startTimestamp)
	{
		initIfNeeded();
		event.setStartTimestamp(startTimestamp);
		return this;
	}
	
	public TestEventToStoreBuilder endTimestamp(Instant endTimestamp)
	{
		initIfNeeded();
		event.setEndTimestamp(endTimestamp);
		return this;
	}
	
	public TestEventToStoreBuilder success(boolean success)
	{
		initIfNeeded();
		event.setSuccess(success);
		return this;
	}
	
	public TestEventToStoreBuilder content(byte[] content)
	{
		initIfNeeded();
		event.setContent(content);
		return this;
	}
	
	
	public TestEventToStore build()
	{
		initIfNeeded();
		TestEventToStore result = event;
		event = null;
		return result;
	}
}
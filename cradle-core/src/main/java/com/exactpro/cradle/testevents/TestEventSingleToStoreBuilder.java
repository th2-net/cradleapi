/*
 * Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.exactpro.cradle.testevents;

import java.time.Instant;

/**
 * Builder for {@link TestEventSingleToStore} object. After calling {@link #build()} method, the builder can be reused to build new test event
 */
public class TestEventSingleToStoreBuilder
{
	private TestEventSingleToStore event;
	
	public TestEventSingleToStoreBuilder()
	{
		event = createTestEventToStore();
	}
	
	
	protected TestEventSingleToStore createTestEventToStore()
	{
		return new TestEventSingleToStore();
	}
	
	private void initIfNeeded()
	{
		if (event == null)
			event = createTestEventToStore();
	}
	
	
	public TestEventSingleToStoreBuilder id(StoredTestEventId id)
	{
		initIfNeeded();
		event.setId(id);
		return this;
	}
	
	public TestEventSingleToStoreBuilder name(String name)
	{
		initIfNeeded();
		event.setName(name);
		return this;
	}
	
	public TestEventSingleToStoreBuilder type(String type)
	{
		initIfNeeded();
		event.setType(type);
		return this;
	}
	
	public TestEventSingleToStoreBuilder parentId(StoredTestEventId parentId)
	{
		initIfNeeded();
		event.setParentId(parentId);
		return this;
	}
	
	public TestEventSingleToStoreBuilder endTimestamp(Instant endTimestamp)
	{
		initIfNeeded();
		event.setEndTimestamp(endTimestamp);
		return this;
	}
	
	public TestEventSingleToStoreBuilder success(boolean success)
	{
		initIfNeeded();
		event.setSuccess(success);
		return this;
	}
	
	public TestEventSingleToStoreBuilder content(byte[] content)
	{
		initIfNeeded();
		event.setContent(content);
		return this;
	}
	
	
	public TestEventSingleToStore build()
	{
		initIfNeeded();
		TestEventSingleToStore result = event;
		event = null;
		return result;
	}
}

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

import com.exactpro.cradle.messages.StoredMessageId;

import java.time.Instant;
import java.util.Collection;

/**
 * Builder for {@link TestEventToStore} object. After calling {@link #build()} method, the builder can be reused to build new test event
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

	public TestEventToStoreBuilder messageIds(Collection<StoredMessageId> messageIds)
	{
		initIfNeeded();
		event.setMessageIds(messageIds);
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

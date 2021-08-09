/*
 * Copyright 2020-2021 Exactpro (Exactpro Systems Limited)
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
import java.util.UUID;

import com.exactpro.cradle.BookId;

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
	
	public TestEventBatchToStoreBuilder idRandom(BookId book)
	{
		initIfNeeded();
		event.setId(new StoredTestEventId(book, Instant.now(), UUID.randomUUID().toString()));
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

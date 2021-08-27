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
import java.util.HashSet;
import java.util.Set;

import com.exactpro.cradle.BookId;
import com.exactpro.cradle.messages.StoredMessageId;
import com.exactpro.cradle.utils.CradleStorageException;

/**
 * Builder for {@link TestEventSingleToStore} object. After calling {@link #build()} method, the builder can be reused to build new test event
 */
public class TestEventSingleToStoreBuilder
{
	private StoredTestEventId id;
	private String name;
	private StoredTestEventId parentId;
	private String type;
	private Instant endTimestamp;
	private boolean success;
	private Set<StoredMessageId> messages;
	private byte[] content;
	
	public TestEventSingleToStoreBuilder id(StoredTestEventId id)
	{
		this.id = id;
		return this;
	}
	
	public TestEventSingleToStoreBuilder id(BookId book, String scope, Instant startTimestamp, String id)
	{
		this.id = new StoredTestEventId(book, scope, startTimestamp, id);
		return this;
	}
	
	public TestEventSingleToStoreBuilder name(String name)
	{
		this.name = name;
		return this;
	}
	
	public TestEventSingleToStoreBuilder parentId(StoredTestEventId parentId)
	{
		this.parentId = parentId;
		return this;
	}
	
	public TestEventSingleToStoreBuilder type(String type)
	{
		this.type = type;
		return this;
	}
	
	public TestEventSingleToStoreBuilder endTimestamp(Instant endTimestamp)
	{
		this.endTimestamp = endTimestamp;
		return this;
	}
	
	public TestEventSingleToStoreBuilder success(boolean success)
	{
		this.success = success;
		return this;
	}
	
	public TestEventSingleToStoreBuilder messages(Set<StoredMessageId> ids)
	{
		this.messages = ids;
		return this;
	}
	
	public TestEventSingleToStoreBuilder message(StoredMessageId id)
	{
		if (messages == null)
			messages = new HashSet<>();
		messages.add(id);
		return this;
	}
	
	public TestEventSingleToStoreBuilder content(byte[] content)
	{
		this.content = content;
		return this;
	}
	
	
	public TestEventSingleToStore build() throws CradleStorageException
	{
		try
		{
			TestEventSingleToStore result = createTestEventToStore(id, name, parentId);
			result.setType(type);
			result.setEndTimestamp(endTimestamp);
			result.setSuccess(success);
			result.setMessages(messages);
			result.setContent(content);
			return result;
		}
		finally
		{
			reset();
		}
	}
	
	
	protected TestEventSingleToStore createTestEventToStore(StoredTestEventId id, String name, StoredTestEventId parentId) throws CradleStorageException
	{
		return new TestEventSingleToStore(id, name, parentId);
	}
	
	protected void reset()
	{
		id = null;
		name = null;
		parentId = null;
		type = null;
		endTimestamp = null;
		success = false;
		messages = null;
		content = null;
	}
}

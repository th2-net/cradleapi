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
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import com.exactpro.cradle.BookId;
import com.exactpro.cradle.messages.StoredMessageId;
import com.exactpro.cradle.utils.CradleStorageException;

/**
 * Holds information about test event stored in Cradle.
 * Can be a single event or a batch of events.
 * Use {@link #isSingle()} or {@link #isBatch()} to determine how to treat the event.
 * Depending on the result, use {@link #asSingle()} or {@link #asBatch()} to work with event as a single event or as a batch, respectively
 */
public abstract class StoredTestEvent implements TestEvent
{
	private final StoredTestEventId id;
	private final String name,
			type;
	private final StoredTestEventId parentId;
	protected final Set<StoredMessageId> messages;
	
	public StoredTestEvent(StoredTestEventId id, String name, String type, StoredTestEventId parentId) throws CradleStorageException
	{
		this.id = id;
		this.name = name;
		this.type = type;
		this.parentId = parentId;
		
		if (this.id == null)
			throw new CradleStorageException("Test event ID cannot be null");
		if (this.id.equals(parentId))
			throw new CradleStorageException("Test event cannot reference itself");
		
		this.messages = new HashSet<>();
	}
	
	public StoredTestEvent(TestEvent event) throws CradleStorageException
	{
		this(event.getId(), event.getName(), event.getType(), event.getParentId());
	}
	
	
	public static StoredTestEventSingle single(TestEventSingleToStore event) throws CradleStorageException
	{
		return new StoredTestEventSingle(event);
	}
	
	public static StoredTestEventBatch batch(TestEventBatchToStore event) throws CradleStorageException
	{
		return new StoredTestEventBatch(event);
	}
	
	public static StoredTestEventSingle single(TestEventSingleToStoreBuilder builder) throws CradleStorageException
	{
		return new StoredTestEventSingle(builder.build());
	}
	
	public static StoredTestEventBatch batch(TestEventBatchToStoreBuilder builder) throws CradleStorageException
	{
		return new StoredTestEventBatch(builder.build());
	}
	
	public static TestEventSingleToStoreBuilder singleBuilder()
	{
		return new TestEventSingleToStoreBuilder();
	}
	
	public static TestEventBatchToStoreBuilder batchBuilder()
	{
		return new TestEventBatchToStoreBuilder();
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
	
	@Override
	public final BookId getBookId()
	{
		return TestEvent.bookId(this);
	}
	
	@Override
	public final String getScope()
	{
		return TestEvent.scope(this);
	}
	
	@Override
	public final Instant getStartTimestamp()
	{
		return TestEvent.startTimestamp(this);
	}
	
	@Override
	public final Set<StoredMessageId> getMessages()
	{
		return Collections.unmodifiableSet(messages);
	}
	
	
	public final boolean isSingle()
	{
		return this instanceof StoredTestEventSingle;
	}
	
	public final boolean isBatch()
	{
		return this instanceof StoredTestEventBatch;
	}
	
	public final StoredTestEventSingle asSingle()
	{
		return (StoredTestEventSingle)this;
	}
	
	public final StoredTestEventBatch asBatch()
	{
		return (StoredTestEventBatch)this;
	}
}

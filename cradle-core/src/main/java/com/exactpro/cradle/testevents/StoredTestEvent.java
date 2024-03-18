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

import com.exactpro.cradle.PageId;
import com.exactpro.cradle.utils.CradleStorageException;

import java.time.Instant;

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
	private final PageId pageId;
	private final String error;
	private final Instant recDate;
	
	public StoredTestEvent(StoredTestEventId id, String name, String type, StoredTestEventId parentId, PageId pageId, String error, Instant recDate)
	{
		this.id = id;
		this.name = name;
		this.type = type;
		this.parentId = parentId;
		this.pageId = pageId;
		this.error = error;
		this.recDate = recDate;
	}
	
	public StoredTestEvent(TestEvent event, PageId pageId)
	{
		this(event.getId(), event.getName(), event.getType(), event.getParentId(), pageId, null, null);
	}


	public static StoredTestEventSingle single(TestEventSingleToStore event, PageId pageId) throws CradleStorageException
	{
		return new StoredTestEventSingle(event, pageId);
	}
	
//	public static StoredTestEventBatch batch(TestEventBatchToStore event, PageId pageId) throws CradleStorageException
//	{
//		return new StoredTestEventBatch(event, pageId);
//	}
	
	
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
	
	
	public PageId getPageId()
	{
		return pageId;
	}
	
	public String getError()
	{
		return error;
	}

	public Instant getRecDate() {
		return recDate;
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

	public abstract Instant getLastStartTimestamp();
}

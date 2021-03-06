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

import java.io.Serializable;
import java.time.Instant;
import java.util.Collection;

/**
 * Holds information about one test event stored in batch of events ({@link StoredTestEventBatch})
 */
public class BatchedStoredTestEvent implements StoredTestEventWithContent, Serializable
{
	private static final long serialVersionUID = 1052669983594471856L;
	
	private final StoredTestEventId id;
	private final String name,
			type;
	private final StoredTestEventId parentId;
	private final Instant startTimestamp,
			endTimestamp;
	private final boolean success;
	private final byte[] content;
	
	private final transient StoredTestEventBatch batch;
	
	public BatchedStoredTestEvent(StoredTestEventWithContent event, StoredTestEventBatch batch)
	{
		this.id = event.getId();
		this.name = event.getName();
		this.type = event.getType();
		this.parentId = event.getParentId();
		this.startTimestamp = event.getStartTimestamp();
		this.endTimestamp = event.getEndTimestamp();
		this.success = event.isSuccess();
		this.content = event.getContent();
		
		this.batch = batch;
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
	public Instant getStartTimestamp()
	{
		return startTimestamp;
	}
	
	@Override
	public Instant getEndTimestamp()
	{
		return endTimestamp;
	}
	
	@Override
	public boolean isSuccess()
	{
		return success;
	}
	
	@Override
	public byte[] getContent()
	{
		return content;
	}
	
	
	public StoredTestEventId getBatchId()
	{
		return batch.getId();
	}
	
	public boolean hasChildren()
	{
		return batch.hasChildren(getId());
	}
	
	public Collection<BatchedStoredTestEvent> getChildren()
	{
		return batch.getChildren(getId());
	}
}

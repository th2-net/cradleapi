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
 * Holds metadata of one test event stored in batch of events ({@link StoredTestEventBatch})
 */
public class BatchedStoredTestEventMetadata implements StoredTestEvent, Serializable
{
	private static final long serialVersionUID = 4011304519292482526L;
	
	private final StoredTestEventId id;
	private final String name,
			type;
	private final StoredTestEventId parentId;
	private final Instant startTimestamp,
			endTimestamp;
	private final boolean success;
	
	private final transient StoredTestEventBatchMetadata batch;
	
	public BatchedStoredTestEventMetadata(StoredTestEvent event, StoredTestEventBatchMetadata batch)
	{
		this.id = event.getId();
		this.name = event.getName();
		this.type = event.getType();
		this.parentId = event.getParentId();
		this.startTimestamp = event.getStartTimestamp();
		this.endTimestamp = event.getEndTimestamp();
		this.success = event.isSuccess();
		
		this.batch = batch;
	}

	protected BatchedStoredTestEventMetadata(StoredTestEventId id, String name, String type, StoredTestEventId parentId, Instant startTimestamp, Instant endTimestamp, boolean success, StoredTestEventBatchMetadata batch) {
		this.id = id;
		this.name = name;
		this.type = type;
		this.parentId = parentId;
		this.startTimestamp = startTimestamp;
		this.endTimestamp = endTimestamp;
		this.success = success;
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
	
	
	public StoredTestEventId getBatchId()
	{
		return batch.getId();
	}
	
	public boolean hasChildren()
	{
		return batch.hasChildren(getId());
	}
	
	public Collection<BatchedStoredTestEventMetadata> getChildren()
	{
		return batch.getChildren(getId());
	}
}

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

import java.io.IOException;
import java.time.Instant;

import com.exactpro.cradle.utils.TestEventUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StoredTestEventMetadata implements StoredTestEvent
{
	private final Logger logger = LoggerFactory.getLogger(StoredTestEventMetadata.class);

	private StoredTestEventId id;
	private String name,
			type;
	private StoredTestEventId parentId;
	private Instant startTimestamp,
			endTimestamp;
	private boolean success,
			batch;
	private int eventCount;
	private byte[] batchMetadataBytes;
	private StoredTestEventBatchMetadata batchMetadata;
	
	public StoredTestEventMetadata()
	{
	}
	
	public StoredTestEventMetadata(StoredTestEvent event)
	{
		this.id = event.getId();
		this.name = event.getName();
		this.type = event.getType();
		this.parentId = event.getParentId();
		this.startTimestamp = event.getStartTimestamp();
		this.endTimestamp = event.getEndTimestamp();
		this.success = event.isSuccess();
		
		if (event instanceof StoredTestEventBatch)
		{
			StoredTestEventBatch eventBatch = (StoredTestEventBatch)event;
			this.batch = true;
			this.eventCount = eventBatch.getTestEventsCount();
			this.batchMetadata = eventBatch.getTestEventsMetadata();
		}
		else
		{
			this.batch = false;
			this.eventCount = 1;
			this.batchMetadata = null;
		}
	}
	
	
	public StoredTestEventId getId()
	{
		return id;
	}
	
	public void setId(StoredTestEventId id)
	{
		this.id = id;
	}
	
	
	public String getName()
	{
		return name;
	}
	
	public void setName(String name)
	{
		this.name = name;
	}
	
	
	public String getType()
	{
		return type;
	}
	
	public void setType(String type)
	{
		this.type = type;
	}
	
	
	public StoredTestEventId getParentId()
	{
		return parentId;
	}
	
	public void setParentId(StoredTestEventId parentId)
	{
		this.parentId = parentId;
	}
	
	
	public Instant getStartTimestamp()
	{
		return startTimestamp;
	}
	
	public void setStartTimestamp(Instant startTimestamp)
	{
		this.startTimestamp = startTimestamp;
	}
	
	
	public Instant getEndTimestamp()
	{
		return endTimestamp;
	}
	
	public void setEndTimestamp(Instant endTimestamp)
	{
		this.endTimestamp = endTimestamp;
	}
	
	
	public boolean isSuccess()
	{
		return success;
	}
	
	public void setSuccess(boolean success)
	{
		this.success = success;
	}
	
	
	public boolean isBatch()
	{
		return batch;
	}
	
	public void setBatch(boolean batch)
	{
		this.batch = batch;
	}
	
	
	public int getEventCount()
	{
		return eventCount;
	}
	
	public void setEventCount(int eventCount)
	{
		this.eventCount = eventCount;
	}
	
	
	public StoredTestEventBatchMetadata getBatchMetadata() throws IOException
	{
		if (batchMetadata != null || batchMetadataBytes == null)
			return batchMetadata;
		
		synchronized (this)
		{
			try
			{
				StoredTestEventBatchMetadata metadata = new StoredTestEventBatchMetadata(getId(), getParentId());
				batchMetadata = metadata;
				TestEventUtils.deserializeTestEventsMetadata(batchMetadataBytes, metadata);
				batchMetadataBytes = null;
			}
			catch (IOException e)
			{
				throw new IOException("Error while deserializing test events metadata", e);
			}
		}
		return batchMetadata;
	}
	
	public void setBatchMetadata(StoredTestEventBatchMetadata m)
	{
		this.batchMetadata = m;
	}
	
	public void setBatchMetadataBytes(byte[] batchMetadataBytes)
	{
		this.batchMetadataBytes = batchMetadataBytes;
	}

	public Instant getMaxStartTimestamp () {
		Instant maxStart = getStartTimestamp();
		try {
			for (BatchedStoredTestEventMetadata event : getBatchMetadata().getTestEvents()) {
				if (maxStart == null || maxStart.isBefore(event.getStartTimestamp())) {
					maxStart = event.getStartTimestamp();
				}
			}
		} catch (IOException e) {
			logger.warn("Could not get max timestamp from batch metadata: " + e.getMessage());
		}

		return maxStart;
	}
}

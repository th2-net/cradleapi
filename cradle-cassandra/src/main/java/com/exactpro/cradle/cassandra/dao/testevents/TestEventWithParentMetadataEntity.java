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

package com.exactpro.cradle.cassandra.dao.testevents;

import static com.exactpro.cradle.cassandra.StorageConstants.EVENT_BATCH_METADATA;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.UUID;

import com.datastax.oss.driver.api.mapper.annotations.CqlName;
import com.exactpro.cradle.testevents.StoredTestEvent;
import com.exactpro.cradle.testevents.StoredTestEventBatch;
import com.exactpro.cradle.testevents.StoredTestEventBatchMetadata;
import com.exactpro.cradle.testevents.StoredTestEventId;
import com.exactpro.cradle.testevents.StoredTestEventMetadata;
import com.exactpro.cradle.utils.TestEventUtils;

/**
 * Contains metadata of test event with parent to extend with partition and clustering fields
 */
public abstract class TestEventWithParentMetadataEntity extends TestEventMetadataEntity
{
	@CqlName(EVENT_BATCH_METADATA)
	private ByteBuffer eventBatchMetadata;
	
	public TestEventWithParentMetadataEntity()
	{
	}
	
	public TestEventWithParentMetadataEntity(StoredTestEvent event, UUID instanceId) throws IOException
	{
		super(event, instanceId);
		
		StoredTestEventId parentId = event.getParentId();
		this.setRoot(parentId == null);
		this.setParentId(parentId != null ? parentId.toString() : null);
		
		if (event instanceof StoredTestEventBatch)
		{
			StoredTestEventBatch batch = (StoredTestEventBatch)event;
			byte[] metadata = TestEventUtils.serializeTestEventsMetadata(batch.getTestEventsMetadata().getTestEvents());
			this.setEventBatchMetadata(ByteBuffer.wrap(metadata));
		}
		else
			this.eventBatchMetadata = null;
	}
	
	
	public abstract boolean isRoot();
	public abstract void setRoot(boolean root);
	
	public abstract String getParentId();
	public abstract void setParentId(String parentId);
	
	
	public ByteBuffer getEventBatchMetadata()
	{
		return eventBatchMetadata;
	}
	
	public void setEventBatchMetadata(ByteBuffer eventBatchMetadata)
	{
		this.eventBatchMetadata = eventBatchMetadata;
	}
	
	
	@Override
	public StoredTestEventMetadata toStoredTestEventMetadata() throws IOException
	{
		StoredTestEventMetadata result =  super.toStoredTestEventMetadata();
		String parentId = getParentId();
		if (parentId != null)
			result.setParentId(new StoredTestEventId(parentId));
		
		if (eventBatchMetadata == null)
			return result;
		
		try
		{
			StoredTestEventBatchMetadata metadata = new StoredTestEventBatchMetadata(result.getId(), result.getParentId());
			result.setBatchMetadata(metadata);
			TestEventUtils.deserializeTestEventsMetadata(eventBatchMetadata.array(), metadata);
		}
		catch (IOException e)
		{
			throw new IOException("Error while deserializing test events metadata", e);
		}
		return result;
	}
}

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

package com.exactpro.cradle.cassandra.dao.testevents;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.UUID;

import com.datastax.oss.driver.api.mapper.annotations.*;
import com.exactpro.cradle.utils.TestEventUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.cradle.cassandra.CassandraCradleStorage;
import com.exactpro.cradle.testevents.StoredTestEvent;
import com.exactpro.cradle.testevents.StoredTestEventBatch;
import com.exactpro.cradle.testevents.StoredTestEventId;
import com.exactpro.cradle.testevents.StoredTestEventMetadata;

import static com.exactpro.cradle.cassandra.StorageConstants.*;

/**
 * Contains test event metadata
 */
@Entity
public class TestEventMetadataEntity
{
	private static final Logger logger = LoggerFactory.getLogger(TestEventMetadataEntity.class);

	@PartitionKey(0)
	@CqlName(INSTANCE_ID)
	private UUID instanceId;

	@PartitionKey(1)
	@CqlName(START_DATE)
	private LocalDate startDate;

	@ClusteringColumn(0)
	@CqlName(START_TIME)
	private LocalTime startTime;

	@ClusteringColumn(1)
	@CqlName(ID)
	private String id;

	@CqlName(NAME)
	private String name;
	
	@CqlName(TYPE)
	private String type;
	
	@CqlName(EVENT_BATCH)
	private boolean eventBatch;
	
	@CqlName(END_DATE)
	private LocalDate endDate;
	@CqlName(END_TIME)
	private LocalTime endTime;
	
	@CqlName(SUCCESS)
	private boolean success;
	
	@CqlName(EVENT_COUNT)
	private int eventCount;

	@CqlName(EVENT_BATCH_METADATA)
	private ByteBuffer eventBatchMetadata;

	@CqlName(ROOT)
	private boolean root;

	@CqlName(PARENT_ID)
	private String parentId;


	public TestEventMetadataEntity()
	{
	}

	public TestEventMetadataEntity(StoredTestEvent event, UUID instanceId) throws IOException
	{
		logger.debug("Creating TestEventMetadataEntity from test event '{}'", event.getId());

		StoredTestEventId parentId = event.getParentId();

		this.setInstanceId(instanceId);
		this.setId(event.getId().toString());
		this.setName(event.getName());
		this.setType(event.getType());
		this.setRoot(parentId == null);
		this.parentId = parentId != null ? parentId.toString() : ROOT_EVENT_PARENT_ID;
		this.setStartTimestamp(event.getStartTimestamp());
		this.setEndTimestamp(event.getEndTimestamp());
		this.setSuccess(event.isSuccess());

		if (event instanceof StoredTestEventBatch)
		{
			StoredTestEventBatch batch = (StoredTestEventBatch)event;
			this.setEventBatch(true);
			this.setEventCount(batch.getTestEventsCount());
			
			byte[] metadata = TestEventUtils.serializeTestEventsMetadata(batch.getTestEventsMetadata().getTestEvents());
			this.setEventBatchMetadata(ByteBuffer.wrap(metadata));
		}
		else
		{
			this.setEventBatch(false);
			this.setEventCount(1);
			this.eventBatchMetadata = null;
		}
	}


	public UUID getInstanceId()
	{
		return instanceId;
	}

	public void setInstanceId(UUID instanceId)
	{
		this.instanceId = instanceId;
	}


	public String getId()
	{
		return id;
	}

	public void setId(String id)
	{
		this.id = id;
	}

	public boolean isRoot()
	{
		return root;
	}

	public void setRoot(boolean root)
	{
		this.root = root;
	}


	public String getParentId()
	{
		return parentId;
	}

	public void setParentId(String parentId)  //This is called by Cassandra Driver and is not supposed to be called explicitly
	{
		this.parentId = parentId != null && parentId.isEmpty() ? null : parentId;
	}


	public LocalDate getStartDate()
	{
		return startDate;
	}

	public void setStartDate(LocalDate startDate)
	{
		this.startDate = startDate;
	}


	public LocalTime getStartTime()
	{
		return startTime;
	}

	public void setStartTime(LocalTime startTime)
	{
		this.startTime = startTime;
	}


	public ByteBuffer getEventBatchMetadata()
	{
		return eventBatchMetadata;
	}

	public void setEventBatchMetadata(ByteBuffer eventBatchMetadata)
	{
		this.eventBatchMetadata = eventBatchMetadata;
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
	
	
	public boolean isEventBatch()
	{
		return eventBatch;
	}
	
	public void setEventBatch(boolean eventBatch)
	{
		this.eventBatch = eventBatch;
	}


	public LocalDate getEndDate()
	{
		return endDate;
	}
	
	public void setEndDate(LocalDate endDate)
	{
		this.endDate = endDate;
	}

	
	public LocalTime getEndTime()
	{
		return endTime;
	}
	
	public void setEndTime(LocalTime endTime)
	{
		this.endTime = endTime;
	}


	public boolean isSuccess()
	{
		return success;
	}
	
	public void setSuccess(boolean success)
	{
		this.success = success;
	}
	
	
	public int getEventCount()
	{
		return eventCount;
	}
	
	public void setEventCount(int eventCount)
	{
		this.eventCount = eventCount;
	}


	@Transient
	public Instant getStartTimestamp()
	{
		LocalDate sd = getStartDate();
		LocalTime st = getStartTime();
		if (sd == null || st == null)
			return null;
		return LocalDateTime.of(sd, st).toInstant(CassandraCradleStorage.TIMEZONE_OFFSET);
	}

	@Transient
	public void setStartTimestamp(Instant timestamp)
	{
		if (timestamp == null)
		{
			setStartDate(null);
			setStartTime(null);
			return;
		}
		LocalDateTime ldt = LocalDateTime.ofInstant(timestamp, CassandraCradleStorage.TIMEZONE_OFFSET);
		setStartDate(ldt.toLocalDate());
		setStartTime(ldt.toLocalTime());
	}


	@Transient
	public Instant getEndTimestamp()
	{
		LocalDate ed = getEndDate();
		LocalTime et = getEndTime();
		if (ed == null || et == null)
			return null;
		return LocalDateTime.of(ed, et).toInstant(CassandraCradleStorage.TIMEZONE_OFFSET);
	}

	@Transient
	public void setEndTimestamp(Instant timestamp)
	{
		if (timestamp == null)
		{
			setEndDate(null);
			setEndTime(null);
			return;
		}
		LocalDateTime ldt = LocalDateTime.ofInstant(timestamp, CassandraCradleStorage.TIMEZONE_OFFSET);
		setEndDate(ldt.toLocalDate());
		setEndTime(ldt.toLocalTime());
	}


	public StoredTestEventMetadata toStoredTestEventMetadata() throws IOException
	{
		StoredTestEventMetadata result =  new StoredTestEventMetadata();
		result.setId(new StoredTestEventId(getId()));
		result.setName(name);
		result.setType(type);

		result.setStartTimestamp(getStartTimestamp());
		result.setEndTimestamp(getEndTimestamp());
		result.setSuccess(success);
		result.setBatch(eventBatch);
		result.setEventCount(eventCount);
		String parentId = getParentId();
		if (parentId != null)
			result.setParentId(new StoredTestEventId(parentId));

		if (eventBatchMetadata == null)
			return result;

		result.setBatchMetadataBytes(eventBatchMetadata.array());
		return result;
	}
}

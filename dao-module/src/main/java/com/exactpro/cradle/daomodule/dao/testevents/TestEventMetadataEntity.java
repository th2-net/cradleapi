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

package com.exactpro.cradle.daomodule.dao.testevents;

import com.datastax.oss.driver.api.mapper.annotations.CqlName;
import com.datastax.oss.driver.api.mapper.annotations.Transient;
import com.exactpro.cradle.cassandra.CassandraCradleStorage;
import com.exactpro.cradle.testevents.StoredTestEvent;
import com.exactpro.cradle.testevents.StoredTestEventBatch;
import com.exactpro.cradle.testevents.StoredTestEventId;
import com.exactpro.cradle.testevents.StoredTestEventMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.UUID;

import static com.exactpro.cradle.cassandra.StorageConstants.*;

/**
 * Contains test event metadata to extend with partition and clustering fields
 */
public abstract class TestEventMetadataEntity
{
	private static final Logger logger = LoggerFactory.getLogger(TestEventMetadataEntity.class);
	
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
	
	
	public TestEventMetadataEntity()
	{
	}
	
	public TestEventMetadataEntity(StoredTestEvent event, UUID instanceId)
	{
		logger.trace("Creating metadata from event");
		
		this.setInstanceId(instanceId);
		this.setStartTimestamp(event.getStartTimestamp());
		this.setId(event.getId().toString());
		
		this.setName(event.getName());
		this.setType(event.getType());
		
		this.setEndTimestamp(event.getEndTimestamp());
		this.setSuccess(event.isSuccess());
		
		if (event instanceof StoredTestEventBatch)
		{
			this.setEventBatch(true);
			this.setEventCount(((StoredTestEventBatch)event).getTestEventsCount());
		}
		else
		{
			this.setEventBatch(false);
			this.setEventCount(1);
		}
	}
	
	
	public abstract UUID getInstanceId();
	public abstract void setInstanceId(UUID instanceId);
	
	public abstract String getId();
	public abstract void setId(String id);
	
	public abstract LocalDate getStartDate();
	public abstract void setStartDate(LocalDate startDate);
	public abstract LocalTime getStartTime();
	public abstract void setStartTime(LocalTime startTime);
	
	
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
			return;
		LocalDateTime ldt = LocalDateTime.ofInstant(timestamp, CassandraCradleStorage.TIMEZONE_OFFSET);
		setStartDate(ldt.toLocalDate());
		setStartTime(ldt.toLocalTime());
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
			return;
		LocalDateTime ldt = LocalDateTime.ofInstant(timestamp, CassandraCradleStorage.TIMEZONE_OFFSET);
		setEndDate(ldt.toLocalDate());
		setEndTime(ldt.toLocalTime());
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
		return result;
	}
}

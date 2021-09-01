/*
 * Copyright 2021-2021 Exactpro (Exactpro Systems Limited)
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

import static com.exactpro.cradle.cassandra.StorageConstants.*;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Set;

import com.datastax.oss.driver.api.mapper.annotations.ClusteringColumn;
import com.datastax.oss.driver.api.mapper.annotations.CqlName;
import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.PartitionKey;
import com.datastax.oss.driver.api.mapper.annotations.Transient;
import com.exactpro.cradle.cassandra.dao.CradleEntity;
import com.exactpro.cradle.cassandra.utils.CassandraTimeUtils;
import com.exactpro.cradle.testevents.StoredTestEvent;
import com.exactpro.cradle.testevents.StoredTestEventId;
import com.exactpro.cradle.testevents.TestEventToStore;
import com.exactpro.cradle.utils.TimeUtils;

/**
 * Contains data of {@link StoredTestEvent} to write to or to obtain from Cassandra
 */
@Entity
public class TestEventEntity extends CradleEntity
{
	@PartitionKey(0)
	@CqlName(PAGE)
	private String page;
	
	@PartitionKey(1)
	@CqlName(START_DATE)
	private LocalDate startDate;
	
	@PartitionKey(2)
	@CqlName(SCOPE)
	private String scope;
	
	@PartitionKey(4)
	@CqlName(PART)
	private String part;
	
	@ClusteringColumn(0)
	@CqlName(START_TIME)
	private LocalTime startTime;
	
	@ClusteringColumn(1)
	@CqlName(ID)
	private String id;
	
	@ClusteringColumn(3)
	@CqlName(CHUNK)
	private int chunk;
	
	@CqlName(NAME)
	private String name;
	
	@CqlName(TYPE)
	private String type;
	
	@CqlName(SUCCESS)
	private boolean success;
	
	@CqlName(ROOT)
	private boolean root;
	
	@CqlName(PARENT_ID)
	private String parentId;

	@CqlName(EVENT_BATCH)
	private boolean eventBatch;
	
	@CqlName(EVENT_COUNT)
	private int eventCount;
	
	@CqlName(END_DATE)
	private LocalDate endDate;
	@CqlName(END_TIME)
	private LocalTime endTime;
	
	@CqlName(MESSAGES)
	private Set<String> messages;
	
	
	public TestEventEntity()
	{
	}
	
	public TestEventEntity(EventEntityData eventData)
	{
		TestEventToStore event = eventData.getEvent();
		StoredTestEventId parentId = event.getParentId();
		LocalDateTime start = TimeUtils.toLocalTimestamp(event.getStartTimestamp());
		
		this.setPage(eventData.getPageId().getName());
		this.setStartTimestamp(start);
		this.setScope(event.getScope());
		this.setPart(CassandraTimeUtils.getPart(start));
		this.setId(event.getId().getId());
		this.setChunk(eventData.getChunk());
		
		this.setSuccess(event.isSuccess());
		this.setRoot(parentId == null);
		this.setEventBatch(event.isBatch());
		if (eventData.getChunk() == 0)
		{
			this.setName(event.getName());
			this.setType(event.getType());
			this.setParentId(parentId != null ? parentId.toString() : "");  //Empty string for absent parentId allows to use index to get root events
			if (event.isBatch())
				this.setEventCount(event.asBatch().getTestEventsCount());
			this.setEndTimestamp(event.getEndTimestamp());
			//TODO: this.setLabels(event.getLabels());
		}
		
		this.setLastChunk(eventData.isLastChunk());
		this.setCompressed(eventData.isCompressed());
		
		this.setMessages(eventData.getMessages());
		if (eventData.getContent() != null)
			this.setContent(ByteBuffer.wrap(eventData.getContent()));
	}
	
	
	@Override
	public String getEntityId()
	{
		return page+StoredTestEventId.ID_PARTS_DELIMITER
				+startDate+StoredTestEventId.ID_PARTS_DELIMITER
				+scope+StoredTestEventId.ID_PARTS_DELIMITER
				+part+StoredTestEventId.ID_PARTS_DELIMITER
				+startTime+StoredTestEventId.ID_PARTS_DELIMITER
				+id;
	}
	
	
	public String getPage()
	{
		return page;
	}
	
	public void setPage(String page)
	{
		this.page = page;
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
	
	@Transient
	public Instant getStartTimestamp()
	{
		LocalDate sd = getStartDate();
		LocalTime st = getStartTime();
		if (sd == null || st == null)
			return null;
		return TimeUtils.fromLocalTimestamp(LocalDateTime.of(sd, st));
	}
	
	@Transient
	public void setStartTimestamp(Instant timestamp)
	{
		if (timestamp != null)
			setStartTimestamp(TimeUtils.toLocalTimestamp(timestamp));
		else
		{
			setStartDate(null);
			setStartTime(null);
		}
	}
	
	@Transient
	public void setStartTimestamp(LocalDateTime timestamp)
	{
		if (timestamp != null)
		{
			setStartDate(timestamp.toLocalDate());
			setStartTime(timestamp.toLocalTime());
		}
		else
		{
			setStartDate(null);
			setStartTime(null);
		}
	}
	
	
	public String getScope()
	{
		return scope;
	}
	
	public void setScope(String scope)
	{
		this.scope = scope;
	}
	
	
	public String getPart()
	{
		return part;
	}
	
	public void setPart(String part)
	{
		this.part = part;
	}
	
	
	public String getId()
	{
		return id;
	}
	
	public void setId(String id)
	{
		this.id = id;
	}
	
	
	public int getChunk()
	{
		return chunk;
	}
	
	public void setChunk(int chunk)
	{
		this.chunk = chunk;
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
	
	
	public boolean isSuccess()
	{
		return success;
	}
	
	public void setSuccess(boolean success)
	{
		this.success = success;
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
	
	public void setParentId(String parentId)
	{
		this.parentId = parentId;
	}
	
	
	public boolean isEventBatch()
	{
		return eventBatch;
	}
	
	public void setEventBatch(boolean eventBatch)
	{
		this.eventBatch = eventBatch;
	}
	
	
	public int getEventCount()
	{
		return eventCount;
	}
	
	public void setEventCount(int eventCount)
	{
		this.eventCount = eventCount;
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
		return TimeUtils.fromLocalTimestamp(LocalDateTime.of(ed, et));
	}
	
	@Transient
	public void setEndTimestamp(Instant timestamp)
	{
		if (timestamp != null)
		{
			LocalDateTime ldt = TimeUtils.toLocalTimestamp(timestamp);
			setEndDate(ldt.toLocalDate());
			setEndTime(ldt.toLocalTime());
		}
		else
		{
			setEndDate(null);
			setEndTime(null);
		}
	}
	

	public Set<String> getMessages()
	{
		return messages;
	}
	
	public void setMessages(Set<String> messages)
	{
		this.messages = messages;
	}
}

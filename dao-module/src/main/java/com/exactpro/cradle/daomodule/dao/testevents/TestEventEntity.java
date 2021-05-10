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
import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.PartitionKey;
import com.datastax.oss.driver.api.mapper.annotations.Transient;
import com.exactpro.cradle.testevents.*;
import com.exactpro.cradle.utils.CompressionUtils;
import com.exactpro.cradle.utils.CradleStorageException;
import com.exactpro.cradle.utils.TestEventUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.UUID;

import static com.exactpro.cradle.daomodule.dao.StorageConstants.*;

/**
 * Contains minimal set of data to obtain from Cassandra to build {@link StoredTestEvent}
 * This class provides basic set of fields and is parent for classes that write {@link StoredTestEvent} to Cassandra
 */
@Entity
public class TestEventEntity
{
	private static final Logger logger = LoggerFactory.getLogger(TestEventEntity.class);
	
	@PartitionKey(0)
	@CqlName(INSTANCE_ID)
	private UUID instanceId;
	
	@PartitionKey(1)
	@CqlName(ID)
	private String id;
	
	@CqlName(NAME)
	private String name;
	
	@CqlName(TYPE)
	private String type;
	
	@CqlName(ROOT)
	private boolean root;
	
	@CqlName(PARENT_ID)
	private String parentId;

	@CqlName(EVENT_BATCH)
	private boolean eventBatch;
	
	@CqlName(START_DATE)
	private LocalDate startDate;
	@CqlName(START_TIME)
	private LocalTime startTime;
	
	@CqlName(END_DATE)
	private LocalDate endDate;
	@CqlName(END_TIME)
	private LocalTime endTime;
	
	@CqlName(SUCCESS)
	private boolean success;
	
	@CqlName(COMPRESSED)
	private boolean compressed;

	@CqlName(CONTENT)
	private ByteBuffer content;
	
	
	public TestEventEntity()
	{
	}
	
	public TestEventEntity(StoredTestEvent event, UUID instanceId) throws IOException
	{
		logger.debug("Creating Entity from test event");
		
		StoredTestEventId parentId = event.getParentId();
		
		this.setInstanceId(instanceId);
		this.setId(event.getId().toString());
		this.setName(event.getName());
		this.setType(event.getType());
		this.setRoot(parentId == null);
		this.setParentId(parentId != null ? parentId.toString() : null);
		
		byte[] content;
		if (event instanceof StoredTestEventBatch)
		{
			content = TestEventUtils.serializeTestEvents(((StoredTestEventBatch)event).getTestEvents());
			this.setEventBatch(true);
		}
		else
		{
			content = ((StoredTestEventSingle)event).getContent();
			this.setEventBatch(false);
		}
		
		this.setStartTimestamp(event.getStartTimestamp());
		this.setEndTimestamp(event.getEndTimestamp());
		this.setSuccess(event.isSuccess());
		
		boolean toCompress = this.isNeedToCompress(content);
		if (toCompress)
		{
			try
			{
				logger.trace("Compressing content of test event", event.getId());
				content = CompressionUtils.compressData(content);
			}
			catch (IOException e)
			{
				throw new IOException(String.format("Could not compress test event contents (ID: '%s') to save in Cradle", 
						event.getId().toString()), e);
			}
		}
		
		this.setCompressed(toCompress);
		this.setContent(ByteBuffer.wrap(content));
	}
	
	
	protected boolean isNeedToCompress(byte[] contentBytes)
	{
		return contentBytes.length > TEST_EVENT_BATCH_SIZE_LIMIT_BYTES;
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
		return LocalDateTime.of(sd, st).toInstant(TIMEZONE_OFFSET);
	}
	
	@Transient
	public void setStartTimestamp(Instant timestamp)
	{
		if (timestamp == null)
			return;
		LocalDateTime ldt = LocalDateTime.ofInstant(timestamp, TIMEZONE_OFFSET);
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
		return LocalDateTime.of(ed, et).toInstant(TIMEZONE_OFFSET);
	}
	
	@Transient
	public void setEndTimestamp(Instant timestamp)
	{
		if (timestamp == null)
			return;
		LocalDateTime ldt = LocalDateTime.ofInstant(timestamp, TIMEZONE_OFFSET);
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
	
	
	public boolean isCompressed()
	{
		return compressed;
	}
	
	public void setCompressed(boolean compressed)
	{
		this.compressed = compressed;
	}
	
	
	public ByteBuffer getContent()
	{
		return content;
	}
	
	public void setContent(ByteBuffer content)
	{
		this.content = content;
	}
	
	
	
	public StoredTestEventSingle toStoredTestEventSingle() throws IOException, CradleStorageException
	{
		if (isEventBatch())
			return null;
		
		StoredTestEventId eventId = new StoredTestEventId(id);
		byte[] eventContent = TestEventUtils.getTestEventContentBytes(content, compressed, eventId);
		return new StoredTestEventSingle(new TestEventToStoreBuilder().id(eventId)
				.name(name)
				.type(type)
				.parentId(parentId != null ? new StoredTestEventId(parentId) : null)
				.startTimestamp(getStartTimestamp())
				.endTimestamp(getEndTimestamp())
				.success(success)
				.content(eventContent)
				.build());
	}
	
	public StoredTestEventBatch toStoredTestEventBatch() throws IOException, CradleStorageException
	{
		if (!isEventBatch())
			return null;
		
		StoredTestEventId eventId = new StoredTestEventId(id);
		StoredTestEventBatch result = new StoredTestEventBatch(new TestEventBatchToStoreBuilder()
				.id(eventId)
				.name(name)
				.type(type)
				.parentId(parentId != null ? new StoredTestEventId(parentId) : null)
				.build());
		try
		{
			TestEventUtils.bytesToTestEvents(content, compressed, result);
		}
		catch (CradleStorageException e)
		{
			throw new IOException("Error while adding deserialized test events to batch", e);
		}
		return result;
	}
	
	public StoredTestEvent toStoredTestEvent() throws IOException, CradleStorageException
	{
		return isEventBatch() ? toStoredTestEventBatch() : toStoredTestEventSingle();
	}
	
	public StoredTestEventWrapper toStoredTestEventWrapper() throws IOException, CradleStorageException
	{
		return new StoredTestEventWrapper(toStoredTestEvent());
	}
}

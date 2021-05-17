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

import com.datastax.oss.driver.api.mapper.annotations.CqlName;
import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.Transient;
import com.exactpro.cradle.cassandra.dao.testevents.TestEventEntity;
import com.exactpro.cradle.testevents.StoredTestEvent;
import com.exactpro.cradle.testevents.StoredTestEventBatch;
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
 * Contains all data about {@link StoredTestEvent} to store in Cassandra
 */
@Entity
public class DetailedTestEventEntity extends TestEventEntity
{
	private static final Logger logger = LoggerFactory.getLogger(DetailedTestEventEntity.class);
	
	@CqlName(STORED_DATE)
	private LocalDate storedDate;
	@CqlName(STORED_TIME)
	private LocalTime storedTime;
	
	@CqlName(EVENT_COUNT)
	private int eventCount;
	
	public DetailedTestEventEntity()
	{
	}
	
	public DetailedTestEventEntity(StoredTestEvent event, UUID instanceId) throws IOException
	{
		super(event, instanceId);
		
		logger.trace("Adding details to Entity");
		//All timestamps should be created from UTC, not simply by using LocalTime.now()!
		this.setStoredTimestamp(Instant.now());
		this.setEventCount(event instanceof StoredTestEventBatch ? ((StoredTestEventBatch)event).getTestEventsCount() : 1);
	}
	
	
	public LocalDate getStoredDate()
	{
		return storedDate;
	}
	
	public void setStoredDate(LocalDate storedDate)
	{
		this.storedDate = storedDate;
	}	
	
	public LocalTime getStoredTime()
	{
		return storedTime;
	}
	
	public void setStoredTime(LocalTime storedTime)
	{
		this.storedTime = storedTime;
	}
	
	@Transient
	public Instant getStoredTimestamp()
	{
		return LocalDateTime.of(getStoredDate(), getStoredTime()).toInstant(TIMEZONE_OFFSET);
	}
	
	@Transient
	public void setStoredTimestamp(Instant timestamp)
	{
		LocalDateTime ldt = LocalDateTime.ofInstant(timestamp, TIMEZONE_OFFSET);
		setStoredDate(ldt.toLocalDate());
		setStoredTime(ldt.toLocalTime());
	}
	
	
	public int getEventCount()
	{
		return eventCount;
	}
	
	public void setEventCount(int eventCount)
	{
		this.eventCount = eventCount;
	}
}

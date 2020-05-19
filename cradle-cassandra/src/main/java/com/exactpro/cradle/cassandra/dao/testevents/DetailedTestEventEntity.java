/******************************************************************************
 * Copyright (c) 2009-2020, Exactpro Systems LLC
 * www.exactpro.com
 * Build Software to Test Software
 *
 * All rights reserved.
 * This is unpublished, licensed software, confidential and proprietary 
 * information which is the property of Exactpro Systems LLC or its licensors.
 ******************************************************************************/

package com.exactpro.cradle.cassandra.dao.testevents;

import static com.exactpro.cradle.cassandra.StorageConstants.*;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.oss.driver.api.mapper.annotations.CqlName;
import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.Transient;
import com.exactpro.cradle.cassandra.CassandraCradleStorage;
import com.exactpro.cradle.testevents.StoredTestEventBatch;
import com.exactpro.cradle.testevents.StoredTestEvent;

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
		return LocalDateTime.of(getStoredDate(), getStoredTime()).toInstant(CassandraCradleStorage.TIMEZONE_OFFSET);
	}
	
	@Transient
	public void setStoredTimestamp(Instant timestamp)
	{
		LocalDateTime ldt = LocalDateTime.ofInstant(timestamp, CassandraCradleStorage.TIMEZONE_OFFSET);
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
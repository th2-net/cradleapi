/******************************************************************************
 * Copyright (c) 2009-2020, Exactpro Systems LLC
 * www.exactpro.com
 * Build Software to Test Software
 *
 * All rights reserved.
 * This is unpublished, licensed software, confidential and proprietary 
 * information which is the property of Exactpro Systems LLC or its licensors.
 ******************************************************************************/

package com.exactpro.cradle.cassandra.dao;

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
import com.exactpro.cradle.messages.StoredMessageBatch;

/**
 * Contains all data about {@link StoredMessageBatch} to store in Cassandra
 */
@Entity
public class DetailedMessageBatchEntity extends MessageBatchEntity
{
	private static final Logger logger = LoggerFactory.getLogger(DetailedMessageBatchEntity.class);
	@CqlName(STORED_DATE)
	private LocalDate storedDate;
	@CqlName(STORED_TIME)
	private LocalTime storedTime;
	
	@CqlName(FIRST_MESSAGE_DATE)
	private LocalDate firstMessagedDate;
	@CqlName(FIRST_MESSAGE_TIME)
	private LocalTime firstMessageTime;
	
	@CqlName(LAST_MESSAGE_DATE)
	private LocalDate lastMessagedDate;
	@CqlName(LAST_MESSAGE_TIME)
	private LocalTime lastMessageTime;
	
	@CqlName(MESSAGE_COUNT)
	private int messageCount;
	
	@CqlName(LAST_MESSAGE_INDEX)
	private long lastMessageIndex;
	
	
	public DetailedMessageBatchEntity()
	{
	}
	
	public DetailedMessageBatchEntity(StoredMessageBatch batch, UUID instanceId) throws IOException
	{
		super(batch, instanceId);
		
		logger.trace("Adding details to Entity");
		//All timestamps should be created from UTC, not simply by using LocalTime.now()!
		this.setStoredTimestamp(Instant.now());
		this.setFirstMessageTimestamp(batch.getFirstTimestamp());
		this.setLastMessageTimestamp(batch.getLastTimestamp());
		this.setMessageCount(batch.getMessageCount());
		this.setLastMessageIndex(batch.getLastMessage().getIndex());
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
	
	
	public LocalDate getFirstMessagedDate()
	{
		return firstMessagedDate;
	}
	
	public void setFirstMessagedDate(LocalDate firstMessagedDate)
	{
		this.firstMessagedDate = firstMessagedDate;
	}
	
	public LocalTime getFirstMessageTime()
	{
		return firstMessageTime;
	}
	
	public void setFirstMessageTime(LocalTime firstMessageTime)
	{
		this.firstMessageTime = firstMessageTime;
	}
	
	@Transient
	public Instant getFirstMessageTimestamp()
	{
		return LocalDateTime.of(getFirstMessagedDate(), getFirstMessageTime()).toInstant(CassandraCradleStorage.TIMEZONE_OFFSET);
	}
	
	@Transient
	public void setFirstMessageTimestamp(Instant timestamp)
	{
		LocalDateTime ldt = LocalDateTime.ofInstant(timestamp, CassandraCradleStorage.TIMEZONE_OFFSET);
		setFirstMessagedDate(ldt.toLocalDate());
		setFirstMessageTime(ldt.toLocalTime());
	}
	
	
	public LocalDate getLastMessagedDate()
	{
		return lastMessagedDate;
	}
	
	public void setLastMessagedDate(LocalDate lastMessagedDate)
	{
		this.lastMessagedDate = lastMessagedDate;
	}
	
	public LocalTime getLastMessageTime()
	{
		return lastMessageTime;
	}
	
	public void setLastMessageTime(LocalTime lastMessageTime)
	{
		this.lastMessageTime = lastMessageTime;
	}
	
	@Transient
	public Instant getLastMessageTimestamp()
	{
		return LocalDateTime.of(getLastMessagedDate(), getLastMessageTime()).toInstant(CassandraCradleStorage.TIMEZONE_OFFSET);
	}
	
	@Transient
	public void setLastMessageTimestamp(Instant timestamp)
	{
		LocalDateTime ldt = LocalDateTime.ofInstant(timestamp, CassandraCradleStorage.TIMEZONE_OFFSET);
		setLastMessagedDate(ldt.toLocalDate());
		setLastMessageTime(ldt.toLocalTime());
	}
	
	
	public int getMessageCount()
	{
		return messageCount;
	}
	
	public void setMessageCount(int messageCount)
	{
		this.messageCount = messageCount;
	}
	
	
	public long getLastMessageIndex()
	{
		return lastMessageIndex;
	}
	
	public void setLastMessageIndex(long lastMessageIndex)
	{
		this.lastMessageIndex = lastMessageIndex;
	}
}
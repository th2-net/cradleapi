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

package com.exactpro.cradle.cassandra.dao.messages;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Set;

import com.datastax.oss.driver.api.mapper.annotations.*;
import com.exactpro.cradle.BookId;
import com.exactpro.cradle.messages.StoredMessageId;
import com.exactpro.cradle.utils.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.cradle.Direction;
import com.exactpro.cradle.messages.StoredMessageBatch;

import static com.exactpro.cradle.cassandra.StorageConstants.*;

/**
 * Contains meta-data fields of message batch to obtain from Cassandra
 */
@Entity
public class MessageBatchMetadataEntity
{
	private static final Logger logger = LoggerFactory.getLogger(MessageBatchMetadataEntity.class);
	
	@PartitionKey(0)
	@CqlName(PAGE)
	private String page;
	
	@PartitionKey(1)
	@CqlName(MESSAGE_DATE)
	private LocalDate messageDate;

	@PartitionKey(2)
	@CqlName(SESSION_ALIAS)
	private String sessionAlias;

	@PartitionKey(3)
	@CqlName(DIRECTION)
	private String direction;

	@PartitionKey(4)
	@CqlName(PART)
	private String part;
	
	@ClusteringColumn(0)
	@CqlName(MESSAGE_TIME)
	private LocalTime messageTime;

	@ClusteringColumn(1)
	@CqlName(SEQUENCE)
	private long sequence;

	@ClusteringColumn(2)
	@CqlName(CHUNK)
	private int chunk;

	@CqlName(LAST_CHUNK)
	private boolean lastChunk;

	@CqlName(STORED_DATE)
	private LocalDate storedDate;

	@CqlName(STORED_TIME)
	private LocalTime storedTime;

	@CqlName(LAST_MESSAGE_DATE)
	private LocalDate lastMessageDate;

	@CqlName(LAST_MESSAGE_TIME)
	private LocalTime lastMessageTime;

	@CqlName(MESSAGE_COUNT)
	private int messageCount;

	@CqlName(LAST_SEQUENCE)
	private long lastSequence;

	@CqlName(COMPRESSED)
	private boolean compressed;

	@CqlName(LABELS)
	private Set<String> labels;
	
	public MessageBatchMetadataEntity()
	{
	}
	
//	public MessageBatchMetadataEntity(StoredMessageBatch batch, String page)
//	{
//		logger.debug("Creating meta-data from message batch");
//		this.setPage(page);
//		
//		StoredMessageId id = batch.getId();
//		LocalDateTime ldt = TimeUtils.toLocalTimestamp(id.getTimestamp());
//		setMessageDate(ldt.toLocalDate());
//		setMessageTime(ldt.toLocalTime());
//		setSessionAlias(id.getSessionAlias());
//		setDirection(id.getDirection().getLabel());
//		setSequence(id.getSequence());
//		//All timestamps should be created from UTC, not simply by using LocalTime.now()!
//		setStoredTimestamp(Instant.now());
//		setFirstMessageTimestamp(batch.getFirstTimestamp());
//		setLastMessageTimestamp(batch.getLastTimestamp());
//		setMessageCount(batch.getMessageCount());
//		setLastSequence(batch.getLastMessage().getSequence());
//	}

	public String getPage()
	{
		return page;
	}

	public void setPage(String page)
	{
		this.page = page;
	}

	public LocalDate getMessageDate()
	{
		return messageDate;
	}

	public void setMessageDate(LocalDate messageDate)
	{
		this.messageDate = messageDate;
	}

	public String getSessionAlias()
	{
		return sessionAlias;
	}

	public void setSessionAlias(String sessionAlias)
	{
		this.sessionAlias = sessionAlias;
	}

	public String getPart()
	{
		return part;
	}

	public void setPart(String part)
	{
		this.part = part;
	}

	public LocalTime getMessageTime()
	{
		return messageTime;
	}

	public void setMessageTime(LocalTime messageTime)
	{
		this.messageTime = messageTime;
	}

	public long getSequence()
	{
		return sequence;
	}

	public void setSequence(long sequence)
	{
		this.sequence = sequence;
	}

	public int getChunk()
	{
		return chunk;
	}

	public void setChunk(int chunk)
	{
		this.chunk = chunk;
	}

	public boolean isLastChunk()
	{
		return lastChunk;
	}

	public void setLastChunk(boolean lastChunk)
	{
		this.lastChunk = lastChunk;
	}

	public Set<String> getLabels()
	{
		return labels;
	}

	public void setLabels(Set<String> labels)
	{
		this.labels = labels;
	}

	public String getDirection()
	{
		return direction;
	}
	
	public void setDirection(String direction)
	{
		this.direction = direction;
	}
	
	
	public boolean isCompressed()
	{
		return compressed;
	}
	
	public void setCompressed(boolean compressed)
	{
		this.compressed = compressed;
	}
	
	
	public StoredMessageId createBatchId(BookId bookId)
	{
		return new StoredMessageId(bookId, getSessionAlias(), Direction.byLabel(getDirection()), 
				TimeUtils.toInstant(messageDate, messageTime), getSequence());
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

	public LocalDate getLastMessageDate()
	{
		return lastMessageDate;
	}

	public void setLastMessageDate(LocalDate lastMessageDate)
	{
		this.lastMessageDate = lastMessageDate;
	}

	public LocalTime getLastMessageTime()
	{
		return lastMessageTime;
	}

	public void setLastMessageTime(LocalTime lastMessageTime)
	{
		this.lastMessageTime = lastMessageTime;
	}

	public int getMessageCount()
	{
		return messageCount;
	}

	public void setMessageCount(int messageCount)
	{
		this.messageCount = messageCount;
	}

	public long getLastSequence()
	{
		return lastSequence;
	}

	public void setLastSequence(long lastSequence)
	{
		this.lastSequence = lastSequence;
	}

	@Transient
	public Instant getStoredTimestamp()
	{
		return TimeUtils.toInstant(getStoredDate(), getStoredTime());
	}

	@Transient
	public void setStoredTimestamp(Instant timestamp)
	{
		LocalDateTime ldt = TimeUtils.toLocalTimestamp(timestamp);
		setStoredDate(ldt.toLocalDate());
		setStoredTime(ldt.toLocalTime());
	}


	@Transient
	public Instant getFirstMessageTimestamp()
	{
		return TimeUtils.toInstant(getMessageDate(), getMessageTime());
	}

	@Transient
	public void setFirstMessageTimestamp(Instant timestamp)
	{
		LocalDateTime ldt = TimeUtils.toLocalTimestamp(timestamp);
		setMessageDate(ldt.toLocalDate());
		setMessageTime(ldt.toLocalTime());
	}

	@Transient
	public Instant getLastMessageTimestamp()
	{
		return TimeUtils.toInstant(getLastMessageDate(), getLastMessageTime());
	}

	@Transient
	public void setLastMessageTimestamp(Instant timestamp)
	{
		LocalDateTime ldt = TimeUtils.toLocalTimestamp(timestamp);
		setLastMessageDate(ldt.toLocalDate());
		setLastMessageTime(ldt.toLocalTime());
	}

}

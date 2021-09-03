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

package com.exactpro.cradle.cassandra.dao.messages;

import static com.exactpro.cradle.cassandra.StorageConstants.*;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.UUID;

import com.exactpro.cradle.messages.MessageToStoreBuilder;
import com.exactpro.cradle.messages.StoredMessage;
import com.exactpro.cradle.messages.StoredMessageMetadata;
import com.exactpro.cradle.utils.CradleStorageException;
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
	private LocalDate firstMessageDate;
	@CqlName(FIRST_MESSAGE_TIME)
	private LocalTime firstMessageTime;
	
	@CqlName(LAST_MESSAGE_DATE)
	private LocalDate lastMessageDate;
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

	// Parameter messageBatch must be created by CradleObjectFactory to have the correct batchSize
	public StoredMessageBatch toStoredMessageBatch(StoredMessageBatch messageBatch)
			throws IOException, CradleStorageException
	{
		for (StoredMessage storedMessage : toStoredMessages())
		{
			MessageToStoreBuilder builder = new MessageToStoreBuilder()
					.content(storedMessage.getContent())
					.direction(storedMessage.getDirection())
					.streamName(storedMessage.getStreamName())
					.timestamp(storedMessage.getTimestamp())
					.index(storedMessage.getIndex());
			StoredMessageMetadata metadata = storedMessage.getMetadata();
			if (metadata != null)
				metadata.toMap().forEach(builder::metadata);

			messageBatch.addMessage(builder.build());
		}

		return messageBatch;
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
	
	
	public LocalDate getFirstMessageDate()
	{
		return firstMessageDate;
	}
	
	public void setFirstMessageDate(LocalDate firstMessageDate)
	{
		this.firstMessageDate = firstMessageDate;
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
		return LocalDateTime.of(getFirstMessageDate(), getFirstMessageTime()).toInstant(CassandraCradleStorage.TIMEZONE_OFFSET);
	}
	
	@Transient
	public void setFirstMessageTimestamp(Instant timestamp)
	{
		LocalDateTime ldt = LocalDateTime.ofInstant(timestamp, CassandraCradleStorage.TIMEZONE_OFFSET);
		setFirstMessageDate(ldt.toLocalDate());
		setFirstMessageTime(ldt.toLocalTime());
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
	
	@Transient
	public Instant getLastMessageTimestamp()
	{
		return LocalDateTime.of(getLastMessageDate(), getLastMessageTime()).toInstant(CassandraCradleStorage.TIMEZONE_OFFSET);
	}
	
	@Transient
	public void setLastMessageTimestamp(Instant timestamp)
	{
		LocalDateTime ldt = LocalDateTime.ofInstant(timestamp, CassandraCradleStorage.TIMEZONE_OFFSET);
		setLastMessageDate(ldt.toLocalDate());
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

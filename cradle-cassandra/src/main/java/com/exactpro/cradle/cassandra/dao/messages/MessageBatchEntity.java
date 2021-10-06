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

import com.datastax.oss.driver.api.mapper.annotations.*;
import com.exactpro.cradle.BookId;
import com.exactpro.cradle.Direction;
import com.exactpro.cradle.PageId;
import com.exactpro.cradle.cassandra.dao.CradleEntity;
import com.exactpro.cradle.cassandra.utils.CassandraTimeUtils;
import com.exactpro.cradle.messages.MessageBatch;
import com.exactpro.cradle.messages.StoredMessageId;
import com.exactpro.cradle.utils.TimeUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

import static com.exactpro.cradle.cassandra.StorageConstants.*;

/**
 * Contains all data about {@link MessageBatch} to store in Cassandra
 */
@Entity
public class MessageBatchEntity extends CradleEntity
{
	private static final Logger logger = LoggerFactory.getLogger(MessageBatchEntity.class);

	@PartitionKey(0)
	@CqlName(PAGE)
	private String page;

	@PartitionKey(1)
	@CqlName(SESSION_ALIAS)
	private String sessionAlias;

	@PartitionKey(2)
	@CqlName(DIRECTION)
	private String direction;

	@ClusteringColumn(0)
	@CqlName(MESSAGE_DATE)
	private LocalDate messageDate;

	@ClusteringColumn(1)
	@CqlName(MESSAGE_TIME)
	private LocalTime messageTime;

	@ClusteringColumn(2)
	@CqlName(SEQUENCE)
	private long sequence;

	@ClusteringColumn(3)
	@CqlName(CHUNK)
	private int chunk;

	@CqlName(LAST_MESSAGE_DATE)
	private LocalDate lastMessageDate;

	@CqlName(LAST_MESSAGE_TIME)
	private LocalTime lastMessageTime;

	@CqlName(MESSAGE_COUNT)
	private int messageCount;

	@CqlName(LAST_SEQUENCE)
	private long lastSequence;
	
	
	public MessageBatchEntity()
	{
	}
	
	public MessageBatchEntity(MessageBatch batch, PageId pageId, byte[] content, boolean compressed)
	{
		this(batch, pageId, content, compressed, 0, true);
	}

	public MessageBatchEntity(MessageBatch batch, PageId pageId, byte[] content, boolean compressed, int chunk, boolean lastChunk)
	{
		setPage(pageId.getName());
		StoredMessageId id = batch.getId();
		LocalDateTime ldt = TimeUtils.toLocalTimestamp(id.getTimestamp());
		setMessageDate(ldt.toLocalDate());
		setMessageTime(ldt.toLocalTime());
		setSessionAlias(id.getSessionAlias());
		setDirection(id.getDirection().getLabel());
		setSequence(id.getSequence());
		//Last sequence is used in the getLastSequenceQuery, that returns last chunk
		setLastSequence(batch.getLastMessage().getSequence());

		//TODO		setStoredTimestamp(Instant.now());
		if (chunk == 0) // It's first chunk
		{
			setFirstMessageTimestamp(batch.getFirstTimestamp());
			setLastMessageTimestamp(batch.getLastTimestamp());
			setMessageCount(batch.getMessageCount());
		}

		//Content related data
		setChunk(chunk);
		setLastChunk(lastChunk);
		setCompressed(compressed);
		setContent(ByteBuffer.wrap(content));
	}

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

	@Override
	public int getChunk()
	{
		return chunk;
	}

	public void setChunk(int chunk)
	{
		this.chunk = chunk;
	}

	public String getDirection()
	{
		return direction;
	}

	public void setDirection(String direction)
	{
		this.direction = direction;
	}

	public StoredMessageId createBatchId(BookId bookId)
	{
		return new StoredMessageId(bookId, getSessionAlias(), Direction.byLabel(getDirection()),
				TimeUtils.toInstant(messageDate, messageTime), getSequence());
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

	@Override
	public String getEntityId()
	{
		String idTimestamp = TimeUtils.toIdTimestamp(getFirstMessageTimestamp());
		return StringUtils.joinWith(StoredMessageId.ID_PARTS_DELIMITER, page, sessionAlias, direction, idTimestamp, sequence);
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

/*
 * Copyright 2021-2022 Exactpro (Exactpro Systems Limited)
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
import com.exactpro.cradle.PageId;
import com.exactpro.cradle.messages.MessageBatch;
import com.exactpro.cradle.serialization.SerializedEntityMetadata;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.List;
import java.util.Set;

import static com.exactpro.cradle.cassandra.StorageConstants.*;

@Entity
public class GroupedMessageBatchEntity
{
	private String group;
	
	@Transient
	private final MessageBatchEntity batchEntity;

	public GroupedMessageBatchEntity()
	{
		this.batchEntity = new MessageBatchEntity();
	}

	public GroupedMessageBatchEntity(MessageBatch batch, PageId pageId, int maxUncompressedSize, String group)
			throws IOException
	{
		this(new MessageBatchEntity(batch, pageId, maxUncompressedSize), group);
	}

	public GroupedMessageBatchEntity(MessageBatchEntity batchEntity, String group)
	{
		this.batchEntity = batchEntity;
		this.group = group;
	}

	@PartitionKey(0)
	@CqlName(PAGE)
	public String getPage()
	{
		return batchEntity.getPage();
	}

	public void setPage(String page)
	{
		batchEntity.setPage(page);
	}
	
	@PartitionKey(1)
	@CqlName(ALIAS_GROUP)
	public String getGroup()
	{
		return group;
	}

	public void setGroup(String group)
	{
		this.group = group;
	}

	@ClusteringColumn(0)
	@CqlName(MESSAGE_DATE)
	public LocalDate getMessageDate()
	{
		return batchEntity.getMessageDate();
	}

	public void setMessageDate(LocalDate messageDate)
	{
		batchEntity.setMessageDate(messageDate);
	}

	@ClusteringColumn(1)
	@CqlName(MESSAGE_TIME)
	public LocalTime getMessageTime()
	{
		return batchEntity.getMessageTime();
	}

	public void setMessageTime(LocalTime messageTime)
	{
		batchEntity.setMessageTime(messageTime);
	}
	
	@ClusteringColumn(2)
	@CqlName(SESSION_ALIAS)
	public String getSessionAlias()
	{
		return batchEntity.getSessionAlias();
	}

	public void setSessionAlias(String sessionAlias)
	{
		batchEntity.setSessionAlias(sessionAlias);
	}
	
	@ClusteringColumn(3)
	@CqlName(DIRECTION)
	public String getDirection()
	{
		return batchEntity.getDirection();
	}

	public void setDirection(String direction)
	{
		batchEntity.setDirection(direction);
	}

	@ClusteringColumn(4)
	@CqlName(SEQUENCE)
	public long getSequence()
	{
		return batchEntity.getSequence();
	}

	public void setSequence(long sequence)
	{
		batchEntity.setSequence(sequence);
	}
	
	@CqlName(COMPRESSED)
	public boolean isCompressed()
	{
		return batchEntity.isCompressed();
	}

	public void setCompressed(boolean compressed)
	{
		batchEntity.setCompressed(compressed);
	}

	@CqlName(LABELS)
	public Set<String> getLabels()
	{
		return batchEntity.getLabels();
	}

	public void setLabels(Set<String> labels)
	{
		batchEntity.setLabels(labels);
	}

	@CqlName(CONTENT)
	public ByteBuffer getContent()
	{
		return batchEntity.getContent();
	}

	public void setContent(ByteBuffer content)
	{
		batchEntity.setContent(content);
	}

	@CqlName(LAST_MESSAGE_DATE)
	public LocalDate getLastMessageDate()
	{
		return batchEntity.getLastMessageDate();
	}

	public void setLastMessageDate(LocalDate lastMessageDate)
	{
		batchEntity.setLastMessageDate(lastMessageDate);
	}

	@CqlName(LAST_MESSAGE_TIME)
	public LocalTime getLastMessageTime()
	{
		return batchEntity.getLastMessageTime();
	}

	public void setLastMessageTime(LocalTime lastMessageTime)
	{
		batchEntity.setLastMessageTime(lastMessageTime);
	}

	@CqlName(MESSAGE_COUNT)
	public int getMessageCount()
	{
		return batchEntity.getMessageCount();
	}

	public void setMessageCount(int messageCount)
	{
		batchEntity.setMessageCount(messageCount);
	}

	@CqlName(LAST_SEQUENCE)
	public long getLastSequence()
	{
		return batchEntity.getLastSequence();
	}

	public void setLastSequence(long lastSequence)
	{
		batchEntity.setLastSequence(lastSequence);
	}

	@Transient
	public Instant getFirstMessageTimestamp()
	{
		return batchEntity.getFirstMessageTimestamp();
	}

	@Transient
	public void setFirstMessageTimestamp(Instant timestamp)
	{
		batchEntity.setFirstMessageTimestamp(timestamp);
	}

	@Transient
	public Instant getLastMessageTimestamp()
	{
		return batchEntity.getLastMessageTimestamp();
	}

	@Transient
	public void setLastMessageTimestamp(Instant timestamp)
	{
		batchEntity.setLastMessageTimestamp(timestamp);
	}

	@Transient
	public List<SerializedEntityMetadata> getSerializedMessageMetadata()
	{
		return batchEntity.getSerializedMessageMetadata();
	}

	@Transient
	public void setSerializedMessageMetadata(
			List<SerializedEntityMetadata> serializedMessageMetadata)
	{
		batchEntity.setSerializedMessageMetadata(serializedMessageMetadata);
	}

	@Transient
	public MessageBatchEntity getMessageBatchEntity()
	{
		return batchEntity;
	}
}

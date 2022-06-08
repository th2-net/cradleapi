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

@Entity
public class GroupedMessageBatchEntity
{
	public static final String FIELD_PAGE = "page";
	public static final String FIELD_ALIAS_GROUP = "alias_group";
	public static final String FIELD_FIRST_MESSAGE_DATE = "first_message_date";
	public static final String FIELD_FIRST_MESSAGE_TIME = "first_message_time";
	public static final String FIELD_COMPRESSED = "compressed";
	public static final String FIELD_LABELS = "labels";
	public static final String FIELD_CONTENT = "z_content";
	public static final String FIELD_LAST_MESSAGE_DATE = "last_message_date";
	public static final String FIELD_LAST_MESSAGE_TIME = "last_message_time";
	public static final String FIELD_MESSAGE_COUNT = "message_count";
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
	@CqlName(FIELD_PAGE)
	public String getPage()
	{
		return batchEntity.getPage();
	}

	public void setPage(String page)
	{
		batchEntity.setPage(page);
	}
	
	@PartitionKey(1)
	@CqlName(FIELD_ALIAS_GROUP)
	public String getGroup()
	{
		return group;
	}

	public void setGroup(String group)
	{
		this.group = group;
	}

	@ClusteringColumn(0)
	@CqlName(FIELD_FIRST_MESSAGE_DATE)
	public LocalDate getFirstMessageDate()
	{
		return batchEntity.getMessageDate();
	}

	public void setFirstMessageDate(LocalDate messageDate)
	{
		batchEntity.setMessageDate(messageDate);
	}

	@ClusteringColumn(1)
	@CqlName(FIELD_FIRST_MESSAGE_TIME)
	public LocalTime getFirstMessageTime()
	{
		return batchEntity.getMessageTime();
	}

	public void setFirstMessageTime(LocalTime messageTime)
	{
		batchEntity.setMessageTime(messageTime);
	}
	

	@CqlName(FIELD_COMPRESSED)
	public boolean isCompressed()
	{
		return batchEntity.isCompressed();
	}

	public void setCompressed(boolean compressed)
	{
		batchEntity.setCompressed(compressed);
	}

	@CqlName(FIELD_LABELS)
	public Set<String> getLabels()
	{
		return batchEntity.getLabels();
	}

	public void setLabels(Set<String> labels)
	{
		batchEntity.setLabels(labels);
	}

	@CqlName(FIELD_CONTENT)
	public ByteBuffer getContent()
	{
		return batchEntity.getContent();
	}

	public void setContent(ByteBuffer content)
	{
		batchEntity.setContent(content);
	}

	@CqlName(FIELD_LAST_MESSAGE_DATE)
	public LocalDate getLastMessageDate()
	{
		return batchEntity.getLastMessageDate();
	}

	public void setLastMessageDate(LocalDate lastMessageDate)
	{
		batchEntity.setLastMessageDate(lastMessageDate);
	}

	@CqlName(FIELD_LAST_MESSAGE_TIME)
	public LocalTime getLastMessageTime()
	{
		return batchEntity.getLastMessageTime();
	}

	public void setLastMessageTime(LocalTime lastMessageTime)
	{
		batchEntity.setLastMessageTime(lastMessageTime);
	}

	@CqlName(FIELD_MESSAGE_COUNT)
	public int getMessageCount()
	{
		return batchEntity.getMessageCount();
	}

	public void setMessageCount(int messageCount)
	{
		batchEntity.setMessageCount(messageCount);
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

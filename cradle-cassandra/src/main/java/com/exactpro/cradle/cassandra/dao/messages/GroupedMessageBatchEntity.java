/*
 * Copyright 2022-2022 Exactpro (Exactpro Systems Limited)
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
import com.exactpro.cradle.messages.*;
import com.exactpro.cradle.utils.CradleStorageException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.UUID;

import static com.exactpro.cradle.cassandra.StorageConstants.*;

@Entity
public class GroupedMessageBatchEntity
{
	private String group;
	
	@Transient
	private final DetailedMessageBatchEntity batchEntity;
	
	public GroupedMessageBatchEntity(StoredMessageBatch batch, UUID instanceId, String group) throws IOException
	{
		this(new DetailedMessageBatchEntity(batch, instanceId), group);
	}
	
	public GroupedMessageBatchEntity(DetailedMessageBatchEntity batchEntity, String group)
	{
		this.batchEntity = batchEntity;
	}
	
	public GroupedMessageBatchEntity()
	{
		this.batchEntity = new DetailedMessageBatchEntity();
	}
	
	@PartitionKey(0)
	@CqlName(INSTANCE_ID)
	public UUID getInstanceId()
	{
		return batchEntity.getInstanceId();
	}
	
	public void setInstanceId(UUID instanceId)
	{
		batchEntity.setInstanceId(instanceId);
	}
	
	@PartitionKey(1)
	@CqlName(STREAM_GROUP)
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
		return batchEntity.getFirstMessageDate();
	}

	public void setMessageDate(LocalDate messageDate)
	{
		batchEntity.setFirstMessageDate(messageDate);
	}

	@ClusteringColumn(1)
	@CqlName(MESSAGE_TIME)
	public LocalTime getMessageTime()
	{
		return batchEntity.getFirstMessageTime();
	}

	public void setMessageTime(LocalTime messageTime)
	{
		batchEntity.setFirstMessageTime(messageTime);
	}

	@ClusteringColumn(2)
	@CqlName(MESSAGE_INDEX)
	public long getMessageIndex()
	{
		return batchEntity.getMessageIndex();
	}
	
	public void setMessageIndex(long messageIndex)
	{
		batchEntity.setMessageIndex(messageIndex);
	}
	
	// Columns
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
	
	@CqlName(COMPRESSED)
	public boolean isCompressed()
	{
		return batchEntity.isCompressed();
	}
	
	public void setCompressed(boolean compressed)
	{
		batchEntity.setCompressed(compressed);
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
	
	@CqlName(STORED_DATE)
	public LocalDate getStoredDate()
	{
		return batchEntity.getStoredDate();
	}
	
	public void setStoredDate(LocalDate storedDate)
	{
		batchEntity.setStoredDate(storedDate);
	}
	
	@CqlName(STORED_TIME)
	public LocalTime getStoredTime()
	{
		return batchEntity.getStoredTime();
	}
	
	public void setStoredTime(LocalTime storedTime)
	{
		batchEntity.setStoredTime(storedTime);
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
	
	@CqlName(LAST_MESSAGE_INDEX)
	public long getLastMessageIndex()
	{
		return batchEntity.getLastMessageIndex();
	}
	
	public void setLastMessageIndex(long lastMessageIndex)
	{
		batchEntity.setLastMessageIndex(lastMessageIndex);
	}
	
	@Transient
	public DetailedMessageBatchEntity getBatchEntity()
	{
		return batchEntity;
	}

	public StoredGroupMessageBatch toStoredGroupMessageBatch() throws IOException, CradleStorageException
	{
		StoredGroupMessageBatch messageBatch = new StoredGroupMessageBatch();
		for (StoredMessage storedMessage : messageBatch.getMessages())
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
	public Instant getFirstMessageTimestamp()
	{
		return batchEntity.getFirstMessageTimestamp();
	}
	
	@Transient
	public void setFirstMessageTimestamp(Instant timestamp)
	{
		batchEntity.setFirstMessageTimestamp(timestamp);
	}
}

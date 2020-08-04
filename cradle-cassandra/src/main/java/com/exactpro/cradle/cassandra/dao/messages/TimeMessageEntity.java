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

import static com.exactpro.cradle.cassandra.StorageConstants.DIRECTION;
import static com.exactpro.cradle.cassandra.StorageConstants.INSTANCE_ID;
import static com.exactpro.cradle.cassandra.StorageConstants.MESSAGE_INDEX;
import static com.exactpro.cradle.cassandra.StorageConstants.MESSAGE_TIMESTAMP;
import static com.exactpro.cradle.cassandra.StorageConstants.STREAM_NAME;

import java.time.Instant;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.oss.driver.api.mapper.annotations.ClusteringColumn;
import com.datastax.oss.driver.api.mapper.annotations.CqlName;
import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.PartitionKey;
import com.exactpro.cradle.Direction;
import com.exactpro.cradle.messages.StoredMessage;
import com.exactpro.cradle.messages.StoredMessageId;
import com.exactpro.cradle.utils.TimeUtils;

/**
 * Contains message data related to given time in seconds
 */
@Entity
public class TimeMessageEntity
{
	private static final Logger logger = LoggerFactory.getLogger(TimeMessageEntity.class);
	
	@PartitionKey(0)
	@CqlName(INSTANCE_ID)
	private UUID instanceId;
	
	@PartitionKey(1)
	@CqlName(MESSAGE_TIMESTAMP)
	private Instant messageTimestamp;
	
	@PartitionKey(2)
	@CqlName(STREAM_NAME)
	private String streamName;
	
	@ClusteringColumn(0)
	@CqlName(DIRECTION)
	private String direction;
	
	@ClusteringColumn(1)
	@CqlName(MESSAGE_INDEX)
	private long messageIndex;
	
	
	public TimeMessageEntity()
	{
	}
	
	public TimeMessageEntity(StoredMessage message, UUID instanceId)
	{
		logger.trace("Creating time-message data from message");
		this.setInstanceId(instanceId);
		this.setMessageTimestamp(message.getTimestamp());
		
		StoredMessageId id = message.getId();
		this.setStreamName(id.getStreamName());
		this.setDirection(id.getDirection().getLabel());
		this.setMessageIndex(id.getIndex());
	}
	
	
	public UUID getInstanceId()
	{
		return instanceId;
	}
	
	public void setInstanceId(UUID instanceId)
	{
		this.instanceId = instanceId;
	}
	
	
	public Instant getMessageTimestamp()
	{
		return messageTimestamp;
	}
	
	public void setMessageTimestamp(Instant messageTimestamp)
	{
		this.messageTimestamp = TimeUtils.cutNanos(messageTimestamp);
	}
	
	
	public String getStreamName()
	{
		return streamName;
	}
	
	public void setStreamName(String streamName)
	{
		this.streamName = streamName;
	}
	
	
	public String getDirection()
	{
		return direction;
	}
	
	public void setDirection(String direction)
	{
		this.direction = direction;
	}
	
	
	public long getMessageIndex()
	{
		return messageIndex;
	}
	
	public void setMessageIndex(long messageIndex)
	{
		this.messageIndex = messageIndex;
	}
	
	
	public StoredMessageId createMessageId()
	{
		return new StoredMessageId(getStreamName(), Direction.byLabel(getDirection()), getMessageIndex());
	}
}
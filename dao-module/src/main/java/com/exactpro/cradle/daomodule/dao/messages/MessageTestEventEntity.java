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

import com.datastax.oss.driver.api.mapper.annotations.ClusteringColumn;
import com.datastax.oss.driver.api.mapper.annotations.CqlName;
import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.PartitionKey;
import com.exactpro.cradle.messages.StoredMessageId;
import com.exactpro.cradle.testevents.StoredTestEventId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.UUID;

import static com.exactpro.cradle.cassandra.StorageConstants.*;

/**
 * Contains ID of message linked with test event by ID
 */
@Entity
public class MessageTestEventEntity
{
	private static final Logger logger = LoggerFactory.getLogger(MessageTestEventEntity.class);
	
	@PartitionKey(0)
	@CqlName(INSTANCE_ID)
	private UUID instanceId;
	
	@PartitionKey(1)
	@CqlName(MESSAGE_ID)
	private String messageId;
	
	@ClusteringColumn(0)
	@CqlName(TEST_EVENT_ID)
	private String eventId;
	
	@CqlName(BATCH_ID)
	private String batchId;
	
	
	public MessageTestEventEntity()
	{
	}
	
	public MessageTestEventEntity(StoredMessageId messageId, StoredTestEventId eventId, StoredTestEventId batchId, UUID instanceId) throws IOException
	{
		logger.debug("Creating entity with message-event link");
		
		this.messageId = messageId.toString();
		this.eventId = eventId.toString();
		if (batchId != null)
			this.batchId = batchId.toString();
		this.instanceId = instanceId;
	}
	
	
	public UUID getInstanceId()
	{
		return instanceId;
	}
	
	public void setInstanceId(UUID instanceId)
	{
		this.instanceId = instanceId;
	}
	
	
	public String getMessageId()
	{
		return messageId;
	}
	
	public void setMessageId(String messageId)
	{
		this.messageId = messageId;
	}
	
	
	public String getEventId()
	{
		return eventId;
	}
	
	public void setEventId(String eventId)
	{
		this.eventId = eventId;
	}
	
	
	public String getBatchId()
	{
		return batchId;
	}
	
	public void setBatchId(String batchId)
	{
		this.batchId = batchId;
	}
}
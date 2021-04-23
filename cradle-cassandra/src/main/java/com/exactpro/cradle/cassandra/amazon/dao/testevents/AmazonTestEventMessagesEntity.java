/*
 * Copyright 2020-2021 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.cradle.cassandra.amazon.dao.testevents;

import com.datastax.oss.driver.api.core.uuid.Uuids;
import com.datastax.oss.driver.api.mapper.annotations.ClusteringColumn;
import com.datastax.oss.driver.api.mapper.annotations.CqlName;
import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.PartitionKey;
import com.exactpro.cradle.cassandra.dao.testevents.ITestEventMessagesEntity;
import com.exactpro.cradle.messages.StoredMessageId;
import com.exactpro.cradle.testevents.StoredTestEventId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.exactpro.cradle.cassandra.StorageConstants.*;
import static com.exactpro.cradle.cassandra.StorageConstants.MESSAGE_IDS;

/**
 * Contains ID of test event linked with messages by ID
 */
@Entity
public class AmazonTestEventMessagesEntity implements ITestEventMessagesEntity
{
	private static final Logger logger = LoggerFactory.getLogger(AmazonTestEventMessagesEntity.class);

	@PartitionKey(0)
	@CqlName(INSTANCE_ID)
	private UUID instanceId;

	@PartitionKey(1)
	@CqlName(TEST_EVENT_ID)
	private String eventId;

	@ClusteringColumn(0)
	@CqlName(ID)
	private UUID id;

	@CqlName(MESSAGE_IDS)
	private Set<String> messageIds;


	public AmazonTestEventMessagesEntity()
	{
		logger.debug("Creating entity with event-messages link");
		// Amazon Keyspaces does not support frozen data type. 
		// In this case we cannot use messagesIds as the clustering key.
		// Instead, we use this column as the clustering key and generate its value on every insert.
		this.id = Uuids.timeBased();
	}

	public AmazonTestEventMessagesEntity(StoredTestEventId eventId, Set<StoredMessageId> messageIds, UUID instanceId)
	{
		this();
		this.eventId = eventId.toString();
		this.messageIds = messageIds.stream().map(StoredMessageId::toString).collect(
				Collectors.toCollection(LinkedHashSet::new));
		this.instanceId = instanceId;
	}


	@Override
	public UUID getInstanceId()
	{
		return instanceId;
	}

	public void setInstanceId(UUID instanceId)
	{
		this.instanceId = instanceId;
	}


	@Override
	public String getEventId()
	{
		return eventId;
	}

	public void setEventId(String testEventId)
	{
		this.eventId = testEventId;
	}


	@Override
	public Set<String> getMessageIds()
	{
		return messageIds;
	}

	public void setMessageIds(Set<String> messageIds)
	{
		this.messageIds = messageIds;
	}


	public UUID getId()
	{
		return id;
	}

	public void setId(UUID id)
	{
		this.id = id;
	}
}

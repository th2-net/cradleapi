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

package com.exactpro.cradle.cassandra.dao.testevents;

import static com.exactpro.cradle.cassandra.CassandraStorageSettings.TEST_EVENT_BATCH_SIZE_LIMIT_BYTES;
import static com.exactpro.cradle.cassandra.StorageConstants.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import com.datastax.oss.driver.api.mapper.annotations.*;
import com.exactpro.cradle.messages.StoredMessageId;
import com.exactpro.cradle.testevents.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.cradle.utils.CompressionUtils;
import com.exactpro.cradle.utils.CradleStorageException;
import com.exactpro.cradle.utils.TestEventUtils;

/**
 * Contains minimal set of data to obtain from Cassandra to build {@link StoredTestEvent}
 * This class provides basic set of fields and is parent for classes that write {@link StoredTestEvent} to Cassandra
 */
@Entity
public class TestEventEntity extends TestEventMetadataEntity
{
	private static final Logger logger = LoggerFactory.getLogger(TestEventEntity.class);
	
	@CqlName(COMPRESSED)
	private boolean compressed;

	@CqlName(CONTENT)
	private ByteBuffer content;

	@CqlName(MESSAGE_IDS)
	private ByteBuffer messageIds;
	
	
	public TestEventEntity()
	{
	}
	
	public TestEventEntity(StoredTestEvent event, UUID instanceId) throws IOException
	{
		super(event, instanceId);
		logger.debug("Creating TestEventEntity from test event");

		byte[] content, messageIds=null;
		if (event instanceof StoredTestEventBatch)
		{
			StoredTestEventBatch batch = (StoredTestEventBatch) event;
			content = TestEventUtils.serializeTestEvents(batch.getTestEvents());
			messageIds = TestEventUtils.serializeLinkedMessageIds(batch.getMessageIdsMap());
		}
		else
		{
			StoredTestEventSingle single = (StoredTestEventSingle) event;
			content = single.getContent();
			messageIds = TestEventUtils.serializeLinkedMessageIds(single.getMessageIds());
		}

		boolean toCompress = this.isNeedToCompress(content);
		if (toCompress)
		{
			try
			{
				logger.trace("Compressing content of test event '{}'", event.getId());
				content = CompressionUtils.compressData(content);
			}
			catch (IOException e)
			{
				throw new IOException(String.format("Could not compress test event contents (ID: '%s') to save in Cradle", 
						event.getId().toString()), e);
			}
		}
		
		this.setCompressed(toCompress);
		this.setContent(ByteBuffer.wrap(content));
		this.setMessageIds(messageIds != null ? ByteBuffer.wrap(messageIds) : null);
	}
	
	
	protected boolean isNeedToCompress(byte[] contentBytes)
	{
		return contentBytes.length > TEST_EVENT_BATCH_SIZE_LIMIT_BYTES;
	}


	public boolean isCompressed()
	{
		return compressed;
	}
	
	public void setCompressed(boolean compressed)
	{
		this.compressed = compressed;
	}
	
	
	public ByteBuffer getContent()
	{
		return content;
	}
	
	public void setContent(ByteBuffer content)
	{
		this.content = content;
	}
	
	
	
	public StoredTestEventSingle toStoredTestEventSingle() throws IOException, CradleStorageException
	{
		if (isEventBatch())
			return null;
		
		StoredTestEventId eventId = new StoredTestEventId(getId());
		byte[] eventContent = TestEventUtils.getTestEventContentBytes(content, compressed, eventId);
		Collection<StoredMessageId> ids = TestEventUtils.deserializeLinkedMessageIds(messageIds);
		return new StoredTestEventSingle(new TestEventToStoreBuilder().id(eventId)
				.name(getName())
				.type(getType())
				.parentId(getParentId() != null ? new StoredTestEventId(getParentId()) : null)
				.startTimestamp(getStartTimestamp())
				.endTimestamp(getEndTimestamp())
				.success(isSuccess())
				.content(eventContent)
				.messageIds(ids)
				.build());
	}
	
	public StoredTestEventBatch toStoredTestEventBatch() throws IOException, CradleStorageException
	{
		if (!isEventBatch())
			return null;
		Map<StoredTestEventId, Collection<StoredMessageId>> ids = TestEventUtils.deserializeLinkedMessageIds(messageIds);
		StoredTestEventId eventId = new StoredTestEventId(getId());
		StoredTestEventBatch result = new StoredTestEventBatch(new TestEventBatchToStoreBuilder()
				.id(eventId)
				.name(getName())
				.type(getType())
				.parentId(getParentId() != null ? new StoredTestEventId(getParentId()) : null)
				.build(), ids);
		try
		{
			TestEventUtils.bytesToTestEvents(content, compressed, result);
		}
		catch (CradleStorageException e)
		{
			throw new IOException("Error while adding deserialized test events to batch", e);
		}
		return result;
	}
	
	public StoredTestEvent toStoredTestEvent() throws IOException, CradleStorageException
	{
		return isEventBatch() ? toStoredTestEventBatch() : toStoredTestEventSingle();
	}
	
	public StoredTestEventWrapper toStoredTestEventWrapper() throws IOException, CradleStorageException
	{
		return new StoredTestEventWrapper(toStoredTestEvent());
	}

	public ByteBuffer getMessageIds()
	{
		return messageIds;
	}

	public void setMessageIds(ByteBuffer messageIds)
	{
		this.messageIds = messageIds;
	}
}

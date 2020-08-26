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

import static com.exactpro.cradle.cassandra.CassandraStorageSettings.MESSAGE_BATCH_SIZE_LIMIT_BYTES;
import static com.exactpro.cradle.cassandra.StorageConstants.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.oss.driver.api.mapper.annotations.CqlName;
import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.exactpro.cradle.messages.StoredMessage;
import com.exactpro.cradle.messages.StoredMessageBatch;
import com.exactpro.cradle.messages.StoredMessageBatchId;
import com.exactpro.cradle.messages.StoredMessageId;
import com.exactpro.cradle.utils.CompressionUtils;
import com.exactpro.cradle.utils.MessageUtils;

/**
 * Contains minimal set of data to obtain from Cassandra to build {@link StoredMessageBatch}
 * This class provides basic set of fields and is parent for classes that write {@link StoredMessageBatch} to Cassandra
 */
@Entity
public class MessageBatchEntity extends MessageBatchMetadataEntity
{
	private static final Logger logger = LoggerFactory.getLogger(MessageBatchEntity.class);
	
	@CqlName(CONTENT)
	private ByteBuffer content;
	
	
	public MessageBatchEntity()
	{
	}
	
	public MessageBatchEntity(StoredMessageBatch batch, UUID instanceId) throws IOException
	{
		super(batch, instanceId);
		logger.debug("Creating Entity with meta-data");
		
		byte[] batchContent = MessageUtils.serializeMessages(batch.getMessages());
		boolean toCompress = this.isNeedToCompress(batchContent);
		if (toCompress)
		{
			StoredMessageBatchId id = batch.getId();
			try
			{
				logger.trace("Compressing content of message batch {}", id);
				batchContent = CompressionUtils.compressData(batchContent);
			}
			catch (IOException e)
			{
				throw new IOException(String.format("Could not compress message batch contents (ID: '%s') to save in Cradle",
						id.toString()), e);
			}
		}
		
		this.setCompressed(toCompress);
		this.setContent(ByteBuffer.wrap(batchContent));
	}
	
	
	protected boolean isNeedToCompress(byte[] contentBytes)
	{
		return contentBytes.length > MESSAGE_BATCH_SIZE_LIMIT_BYTES;
	}


	public ByteBuffer getContent()
	{
		return content;
	}
	
	public void setContent(ByteBuffer content)
	{
		this.content = content;
	}
	
	
	public Collection<StoredMessage> toStoredMessages() throws IOException
	{
		return MessageUtils.bytesToMessages(content, isCompressed());
	}
	
	public StoredMessage toStoredMessage(StoredMessageId id) throws IOException
	{
		return MessageUtils.bytesToOneMessage(content, isCompressed(), id);
	}
}

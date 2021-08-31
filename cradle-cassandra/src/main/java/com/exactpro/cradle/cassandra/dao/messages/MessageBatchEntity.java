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

import static com.exactpro.cradle.cassandra.CassandraStorageSettings.DEFAULT_MAX_UNCOMPRESSED_MESSAGE_BATCH_BYTE_SIZE;
import static com.exactpro.cradle.cassandra.StorageConstants.*;

import java.nio.ByteBuffer;

import com.exactpro.cradle.messages.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.oss.driver.api.mapper.annotations.CqlName;
import com.datastax.oss.driver.api.mapper.annotations.Entity;

/**
 * Contains all data about {@link StoredMessageBatch} to store in Cassandra
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
	
//	public MessageBatchEntity(StoredMessageBatch batch, String page) throws IOException
//	{
//		super(batch, page);
//		logger.debug("Creating Entity with meta-data");
//		
//		byte[] batchContent = MessageUtils.serializeMessages(batch.getMessages());
//		boolean toCompress = this.isNeedToCompress(batchContent);
//		if (toCompress)
//		{
//			StoredMessageId batchId = batch.getId();
//			try
//			{
//				logger.trace("Compressing content of message batch {}", batchId);
//				batchContent = CompressionUtils.compressData(batchContent);
//			}
//			catch (IOException e)
//			{
//				throw new IOException(String.format("Could not compress message batch contents (ID: '%s') to save in Cradle",
//						batchId.toString()), e);
//			}
//		}
//		
//		this.setCompressed(toCompress);
//		this.setContent(ByteBuffer.wrap(batchContent));
//	}
//
//	// Parameter messageBatch must be created by CradleObjectFactory to have the correct batchSize
//	public StoredMessageBatch toStoredMessageBatch(StoredMessageBatch messageBatch)
//			throws IOException, CradleStorageException
//	{
//		for (StoredMessage storedMessage : toStoredMessages())
//		{
//			MessageToStoreBuilder builder = new MessageToStoreBuilder()
//					.content(storedMessage.getContent())
//					.direction(storedMessage.getDirection())
//					.sessionAlias(storedMessage.getSessionAlias())
//					.timestamp(storedMessage.getTimestamp())
//					.sequence(storedMessage.getSequence());
//			StoredMessageMetadata metadata = storedMessage.getMetadata();
//			if (metadata != null)
//				metadata.toMap().forEach(builder::metadata);
//
//			messageBatch.addMessage(builder.build());
//		}
//
//		return messageBatch;
//	}
//
//
	protected boolean isNeedToCompress(byte[] contentBytes)
	{
		return contentBytes.length > DEFAULT_MAX_UNCOMPRESSED_MESSAGE_BATCH_BYTE_SIZE;
	}


	public ByteBuffer getContent()
	{
		return content;
	}

	public void setContent(ByteBuffer content)
	{
		this.content = content;
	}
//	
//	
//	public Collection<StoredMessage> toStoredMessages() throws IOException
//	{
//		return toStoredMessages(Order.DIRECT);
//	}
//
//	public Collection<StoredMessage> toStoredMessages(Order order) throws IOException
//	{
//		List<StoredMessage> messages = MessageUtils.bytesToMessages(content, isCompressed());
//		if (order == Order.DIRECT)
//			return messages;
//		
//		Collections.reverse(messages);
//		return messages;
//	}
//	
//	public StoredMessage toStoredMessage(StoredMessageId id) throws IOException
//	{
//		return MessageUtils.bytesToOneMessage(content, isCompressed(), id);
//	}
}

/******************************************************************************
 * Copyright (c) 2009-2020, Exactpro Systems LLC
 * www.exactpro.com
 * Build Software to Test Software
 *
 * All rights reserved.
 * This is unpublished, licensed software, confidential and proprietary 
 * information which is the property of Exactpro Systems LLC or its licensors.
 ******************************************************************************/

package com.exactpro.cradle.cassandra.dao;

import static com.exactpro.cradle.cassandra.CassandraStorageSettings.MESSAGE_BATCH_SIZE_LIMIT_BYTES;
import static com.exactpro.cradle.cassandra.StorageConstants.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.oss.driver.api.mapper.annotations.ClusteringColumn;
import com.datastax.oss.driver.api.mapper.annotations.CqlName;
import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.PartitionKey;
import com.exactpro.cradle.Direction;
import com.exactpro.cradle.messages.StoredMessage;
import com.exactpro.cradle.messages.StoredMessageBatch;
import com.exactpro.cradle.messages.StoredMessageBatchId;
import com.exactpro.cradle.messages.StoredMessageId;
import com.exactpro.cradle.utils.CompressionUtils;
import com.exactpro.cradle.utils.MessageUtils;

/**
 * Contains minimum set of data to obtain from Cassandra to build {@link StoredMessageBatch}
 * This class provides basic set of fields and is parent for classes that write {@link StoredMessageBatch} to Cassandra
 */
@Entity
public class MessageBatchEntity
{
	private static final Logger logger = LoggerFactory.getLogger(MessageBatchEntity.class);
	
	@PartitionKey(0)
	@CqlName(INSTANCE_ID)
	private UUID instanceId;
	
	@PartitionKey(1)
	@CqlName(STREAM_NAME)
	private String streamName;
	
	@ClusteringColumn(0)
	@CqlName(DIRECTION)
	private String direction;
	
	@ClusteringColumn(1)
	@CqlName(MESSAGE_INDEX)
	private long messageIndex;
	
	@CqlName(COMPRESSED)
	private boolean compressed;

	@CqlName(CONTENT)
	private ByteBuffer content;
	
	
	public MessageBatchEntity()
	{
	}
	
	public MessageBatchEntity(StoredMessageBatch batch, UUID instanceId) throws IOException
	{
		logger.debug("Creating Entity from message batch");
		this.instanceId = instanceId;
		
		StoredMessageBatchId id = batch.getId();
		this.setStreamName(id.getStreamName());
		this.setDirection(id.getDirection().getLabel());
		this.setMessageIndex(id.getIndex());
		
		byte[] batchContent = MessageUtils.serializeMessages(batch.getMessages());
		boolean toCompress = this.isNeedToCompress(batchContent);
		if (toCompress)
		{
			try
			{
				logger.trace("Compressing message batch");
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


	public UUID getInstanceId()
	{
		return instanceId;
	}
	
	public void setInstanceId(UUID instanceId)
	{
		this.instanceId = instanceId;
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
	
	
	public Collection<StoredMessage> toStoredMessages() throws IOException
	{
		return MessageUtils.bytesToMessages(content, compressed, 
				createBatchId());
	}
	
	public StoredMessage toStoredMessage(long index) throws IOException
	{
		return MessageUtils.bytesToOneMessage(content, compressed, 
				new StoredMessageId(createBatchId(), index));
	}
	
	public StoredMessageBatchId createBatchId()
	{
		return new StoredMessageBatchId(streamName, Direction.byLabel(direction), messageIndex);
	}
}
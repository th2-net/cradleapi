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

import com.datastax.oss.driver.api.mapper.annotations.*;
import com.exactpro.cradle.BookId;
import com.exactpro.cradle.Direction;
import com.exactpro.cradle.PageId;
import com.exactpro.cradle.cassandra.dao.CradleEntity;
import com.exactpro.cradle.messages.StoredMessageBatch;
import com.exactpro.cradle.messages.StoredMessageId;
import com.exactpro.cradle.utils.TimeUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

import static com.exactpro.cradle.cassandra.StorageConstants.*;

/**
 * Contains all data about {@link StoredMessageBatch} to store in Cassandra
 */
@Entity
public class MessageBatchEntity extends CradleEntity
{
	private static final Logger logger = LoggerFactory.getLogger(MessageBatchEntity.class);

	@PartitionKey(0)
	@CqlName(PAGE)
	private String page;

	@PartitionKey(1)
	@CqlName(MESSAGE_DATE)
	private LocalDate messageDate;

	@PartitionKey(2)
	@CqlName(SESSION_ALIAS)
	private String sessionAlias;

	@PartitionKey(3)
	@CqlName(DIRECTION)
	private String direction;

	@PartitionKey(4)
	@CqlName(PART)
	private String part;

	@ClusteringColumn(0)
	@CqlName(MESSAGE_TIME)
	private LocalTime messageTime;

	@ClusteringColumn(1)
	@CqlName(SEQUENCE)
	private long sequence;

	@ClusteringColumn(2)
	@CqlName(CHUNK)
	private int chunk;

	@CqlName(LAST_MESSAGE_DATE)
	private LocalDate lastMessageDate;

	@CqlName(LAST_MESSAGE_TIME)
	private LocalTime lastMessageTime;

	@CqlName(MESSAGE_COUNT)
	private int messageCount;

	@CqlName(LAST_SEQUENCE)
	private long lastSequence;
	
	
	public MessageBatchEntity()
	{
	}
	
	public MessageBatchEntity(StoredMessageBatch batch, PageId pageId, byte[] content, boolean compressed)
	{
		this(batch, pageId, content, compressed, 0, true);
	}

	public MessageBatchEntity(StoredMessageBatch batch, PageId pageId, byte[] content, boolean compressed, int chunk, boolean lastChunk)
	{
		setPage(pageId.getName());
		StoredMessageId id = batch.getId();
		LocalDateTime ldt = TimeUtils.toLocalTimestamp(id.getTimestamp());
		setMessageDate(ldt.toLocalDate());
		setMessageTime(ldt.toLocalTime());
		setSessionAlias(id.getSessionAlias());
		setDirection(id.getDirection().getLabel());
		setSequence(id.getSequence());

		//All timestamps should be created from UTC, not simply by using LocalTime.now()!
		//TODO		setStoredTimestamp(Instant.now());
		setFirstMessageTimestamp(batch.getFirstTimestamp());
		setLastMessageTimestamp(batch.getLastTimestamp());
		setMessageCount(batch.getMessageCount());
		setLastSequence(batch.getLastMessage().getSequence());
		
		//Content related data
		setLastChunk(lastChunk);
		setCompressed(compressed);
		setContent(ByteBuffer.wrap(content));
	}

		public String getPage()
	{
		return page;
	}

	public void setPage(String page)
	{
		this.page = page;
	}

	public LocalDate getMessageDate()
	{
		return messageDate;
	}

	public void setMessageDate(LocalDate messageDate)
	{
		this.messageDate = messageDate;
	}

	public String getSessionAlias()
	{
		return sessionAlias;
	}

	public void setSessionAlias(String sessionAlias)
	{
		this.sessionAlias = sessionAlias;
	}

	public String getPart()
	{
		return part;
	}

	public void setPart(String part)
	{
		this.part = part;
	}

	public LocalTime getMessageTime()
	{
		return messageTime;
	}

	public void setMessageTime(LocalTime messageTime)
	{
		this.messageTime = messageTime;
	}

	public long getSequence()
	{
		return sequence;
	}

	public void setSequence(long sequence)
	{
		this.sequence = sequence;
	}

	public int getChunk()
	{
		return chunk;
	}

	public void setChunk(int chunk)
	{
		this.chunk = chunk;
	}

	public String getDirection()
	{
		return direction;
	}

	public void setDirection(String direction)
	{
		this.direction = direction;
	}

	public StoredMessageId createBatchId(BookId bookId)
	{
		return new StoredMessageId(bookId, getSessionAlias(), Direction.byLabel(getDirection()),
				TimeUtils.toInstant(messageDate, messageTime), getSequence());
	}

	public LocalDate getLastMessageDate()
	{
		return lastMessageDate;
	}

	public void setLastMessageDate(LocalDate lastMessageDate)
	{
		this.lastMessageDate = lastMessageDate;
	}

	public LocalTime getLastMessageTime()
	{
		return lastMessageTime;
	}

	public void setLastMessageTime(LocalTime lastMessageTime)
	{
		this.lastMessageTime = lastMessageTime;
	}

	public int getMessageCount()
	{
		return messageCount;
	}

	public void setMessageCount(int messageCount)
	{
		this.messageCount = messageCount;
	}

	public long getLastSequence()
	{
		return lastSequence;
	}

	public void setLastSequence(long lastSequence)
	{
		this.lastSequence = lastSequence;
	}

	@Override
	public String getEntityId()
	{
		return StringUtils.joinWith(StoredMessageId.ID_PARTS_DELIMITER, page, sessionAlias, direction, sequence);
	}

	@Transient
	public Instant getFirstMessageTimestamp()
	{
		return TimeUtils.toInstant(getMessageDate(), getMessageTime());
	}

	@Transient
	public void setFirstMessageTimestamp(Instant timestamp)
	{
		LocalDateTime ldt = TimeUtils.toLocalTimestamp(timestamp);
		setMessageDate(ldt.toLocalDate());
		setMessageTime(ldt.toLocalTime());
	}

	@Transient
	public Instant getLastMessageTimestamp()
	{
		return TimeUtils.toInstant(getLastMessageDate(), getLastMessageTime());
	}

	@Transient
	public void setLastMessageTimestamp(Instant timestamp)
	{
		LocalDateTime ldt = TimeUtils.toLocalTimestamp(timestamp);
		setLastMessageDate(ldt.toLocalDate());
		setLastMessageTime(ldt.toLocalTime());
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
//	protected boolean isNeedToCompress(byte[] contentBytes)
//	{
//		return contentBytes.length > DEFAULT_MAX_UNCOMPRESSED_MESSAGE_BATCH_BYTE_SIZE;
//	}


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

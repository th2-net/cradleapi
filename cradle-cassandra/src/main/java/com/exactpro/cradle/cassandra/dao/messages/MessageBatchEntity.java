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
import com.exactpro.cradle.BookId;
import com.exactpro.cradle.Direction;
import com.exactpro.cradle.PageId;
import com.exactpro.cradle.cassandra.dao.CradleEntity;
import com.exactpro.cradle.messages.MessageBatch;
import com.exactpro.cradle.messages.StoredMessage;
import com.exactpro.cradle.messages.StoredMessageBatch;
import com.exactpro.cradle.messages.StoredMessageId;
import com.exactpro.cradle.serialization.SerializedEntityData;
import com.exactpro.cradle.serialization.SerializedEntityMetadata;
import com.exactpro.cradle.utils.CompressionUtils;
import com.exactpro.cradle.utils.MessageUtils;
import com.exactpro.cradle.utils.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;
import java.util.zip.DataFormatException;


/**
 * Contains all data about {@link MessageBatch} to store in Cassandra
 */
@Entity
@CqlName(MessageBatchEntity.TABLE_NAME)
public class MessageBatchEntity extends CradleEntity
{
	public static final String TABLE_NAME = "messages";
	private static final Logger logger = LoggerFactory.getLogger(MessageBatchEntity.class);

	public static final String FIELD_BOOK = "book";
	public static final String FIELD_PAGE = "page";
	public static final String FIELD_SESSION_ALIAS = "session_alias";
	public static final String FIELD_DIRECTION = "direction";
	public static final String FIELD_FIRST_MESSAGE_DATE = "first_message_date";
	public static final String FIELD_FIRST_MESSAGE_TIME = "first_message_time";
	public static final String FIELD_SEQUENCE = "sequence";
	public static final String FIELD_LAST_MESSAGE_DATE = "last_message_date";
	public static final String FIELD_LAST_MESSAGE_TIME = "last_message_time";
	public static final String FIELD_MESSAGE_COUNT = "message_count";
	public static final String FIELD_LAST_SEQUENCE = "last_sequence";
	public static final String  FIELD_REC_DATE = "rec_date";


	@PartitionKey(1)
	@CqlName(FIELD_BOOK)
	private String book;

	@PartitionKey(2)
	@CqlName(FIELD_PAGE)
	private String page;

	@PartitionKey(3)
	@CqlName(FIELD_SESSION_ALIAS)
	private String sessionAlias;

	@PartitionKey(4)
	@CqlName(FIELD_DIRECTION)
	private String direction;

	@ClusteringColumn(1)
	@CqlName(FIELD_FIRST_MESSAGE_DATE)
	private LocalDate firstMessageDate;

	@ClusteringColumn(2)
	@CqlName(FIELD_FIRST_MESSAGE_TIME)
	private LocalTime firstMessageTime;

	@ClusteringColumn(3)
	@CqlName(FIELD_SEQUENCE)
	private long sequence;

	@CqlName(FIELD_LAST_MESSAGE_DATE)
	private LocalDate lastMessageDate;

	@CqlName(FIELD_LAST_MESSAGE_TIME)
	private LocalTime lastMessageTime;

	@CqlName(FIELD_MESSAGE_COUNT)
	private int messageCount;

	@CqlName(FIELD_LAST_SEQUENCE)
	private long lastSequence;

	@CqlName(FIELD_REC_DATE)
	private Instant recDate;

	private List<SerializedEntityMetadata> serializedMessageMetadata;
	
	public MessageBatchEntity()
	{
	}
	
	public MessageBatchEntity(MessageBatch batch, PageId pageId, int maxUncompressedSize) throws IOException
	{
		logger.debug("Creating entity from message batch '{}'", batch.getId());

		SerializedEntityData serializedEntityData = MessageUtils.serializeMessages(batch.getMessages());

		byte[] batchContent = serializedEntityData.getSerializedData();
		boolean compressed = batchContent.length > maxUncompressedSize;
		if (compressed)
		{
			logger.trace("Compressing content of message batch '{}'", batch.getId());
			batchContent = CompressionUtils.compressData(batchContent);
		}

		setBook(pageId.getBookId().getName());
		setPage(pageId.getName());
		StoredMessageId id = batch.getId();
		LocalDateTime ldt = TimeUtils.toLocalTimestamp(id.getTimestamp());
		setFirstMessageDate(ldt.toLocalDate());
		setFirstMessageTime(ldt.toLocalTime());
		setSessionAlias(id.getSessionAlias());
		setDirection(id.getDirection().getLabel());
		setSequence(id.getSequence());
		//Last sequence is used in the getLastSequenceQuery, that returns last chunk
		setLastSequence(batch.getLastMessage().getSequence());
		
		setFirstMessageTimestamp(batch.getFirstTimestamp());
		setLastMessageTimestamp(batch.getLastTimestamp());
		setMessageCount(batch.getMessageCount());
		
		setCompressed(compressed);
		//TODO: setLabels(batch.getLabels());
		setContent(ByteBuffer.wrap(batchContent));
		setSerializedMessageMetadata(serializedEntityData.getSerializedEntityMetadata());
	}

	public String getBook()
	{
		return book;
	}

	public void setBook(String book)
	{
		this.book = book;
	}

	public String getPage()
	{
		return page;
	}

	public void setPage(String page)
	{
		this.page = page;
	}

	public LocalDate getFirstMessageDate()
	{
		return firstMessageDate;
	}

	public void setFirstMessageDate(LocalDate messageDate)
	{
		this.firstMessageDate = messageDate;
	}

	public String getSessionAlias()
	{
		return sessionAlias;
	}

	public void setSessionAlias(String sessionAlias)
	{
		this.sessionAlias = sessionAlias;
	}

	public LocalTime getFirstMessageTime()
	{
		return firstMessageTime;
	}

	public void setFirstMessageTime(LocalTime messageTime)
	{
		this.firstMessageTime = messageTime;
	}

	public long getSequence()
	{
		return sequence;
	}

	public void setSequence(long sequence)
	{
		this.sequence = sequence;
	}

	public String getDirection()
	{
		return direction;
	}

	public void setDirection(String direction)
	{
		this.direction = direction;
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

	public Instant getRecDate() {
		return recDate;
	}

	public void setRecDate(Instant recDate) {
		this.recDate = recDate;
	}

	@Transient
	public Instant getFirstMessageTimestamp()
	{
		return TimeUtils.toInstant(getFirstMessageDate(), getFirstMessageTime());
	}

	@Transient
	public void setFirstMessageTimestamp(Instant timestamp)
	{
		LocalDateTime ldt = TimeUtils.toLocalTimestamp(timestamp);
		setFirstMessageDate(ldt.toLocalDate());
		setFirstMessageTime(ldt.toLocalTime());
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

	@Transient
	public List<SerializedEntityMetadata> getSerializedMessageMetadata() {
		return serializedMessageMetadata;
	}

	@Transient
	public void setSerializedMessageMetadata(List<SerializedEntityMetadata> serializedMessageMetadata) {
		this.serializedMessageMetadata = serializedMessageMetadata;
	}

	public StoredMessageBatch toStoredMessageBatch(PageId pageId)
			throws DataFormatException, IOException
	{
		StoredMessageId batchId = createId(pageId.getBookId());
		logger.debug("Creating message batch '{}' from entity", batchId);
		
		byte[] content = restoreContent(batchId);
		List<StoredMessage> storedMessages = MessageUtils.deserializeMessages(content, batchId);
		return new StoredMessageBatch(storedMessages, pageId, recDate);
	}
	
	
	public StoredMessageId createId(BookId bookId)
	{
		return new StoredMessageId(bookId, getSessionAlias(), Direction.byLabel(getDirection()),
				TimeUtils.toInstant(firstMessageDate, firstMessageTime), getSequence());
	}
	
	private byte[] restoreContent(StoredMessageId messageBatchId)
			throws DataFormatException, IOException
	{
		ByteBuffer content = getContent();
		if (content == null)
			return null;
		
		byte[] result = content.array();
		if (isCompressed())
		{
			logger.trace("Decompressing content of message batch '{}'", messageBatchId);
			return CompressionUtils.decompressData(result);
		}
		return result;
	}
}

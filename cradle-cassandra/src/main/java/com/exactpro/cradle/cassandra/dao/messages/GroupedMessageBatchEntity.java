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
import com.exactpro.cradle.PageId;
import com.exactpro.cradle.cassandra.dao.CradleEntity;
import com.exactpro.cradle.messages.GroupedMessageBatchToStore;
import com.exactpro.cradle.messages.StoredGroupedMessageBatch;
import com.exactpro.cradle.messages.StoredMessage;
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

@Entity
public class GroupedMessageBatchEntity extends CradleEntity {
	private static final Logger logger = LoggerFactory.getLogger(GroupedMessageBatchEntity.class);

	public static final String FIELD_PAGE = "page";
	public static final String FIELD_ALIAS_GROUP = "alias_group";
	public static final String FIELD_FIRST_MESSAGE_DATE = "first_message_date";
	public static final String FIELD_FIRST_MESSAGE_TIME = "first_message_time";
	public static final String FIELD_LAST_MESSAGE_DATE = "last_message_date";
	public static final String FIELD_LAST_MESSAGE_TIME = "last_message_time";
	public static final String FIELD_MESSAGE_COUNT = "message_count";
	public static final String  FIELD_REC_DATE = "rec_date";

	public GroupedMessageBatchEntity() {
	}

	public GroupedMessageBatchEntity(GroupedMessageBatchToStore batch, PageId pageId, int maxUncompressedSize)
			throws IOException	{

		this.group = batch.getGroup();

		logger.debug("Creating entity from grouped message batch '{}'", group);

		SerializedEntityData serializedEntityData = MessageUtils.serializeMessages(batch.getMessages());

		byte[] batchContent = serializedEntityData.getSerializedData();
		boolean compressed = batchContent.length > maxUncompressedSize;
		if (compressed)	{
			logger.trace("Compressing content of grouped message batch '{}'", group);
			batchContent = CompressionUtils.compressData(batchContent);
		}

		setPage(pageId.getName());
		setFirstMessageTimestamp(batch.getFirstTimestamp());
		setLastMessageTimestamp(batch.getLastTimestamp());
		setMessageCount(batch.getMessageCount());

		setCompressed(compressed);
		//TODO: setLabels(batch.getLabels());
		setContent(ByteBuffer.wrap(batchContent));
		setSerializedMessageMetadata(serializedEntityData.getSerializedEntityMetadata());
	}

	@PartitionKey(0)
	@CqlName(FIELD_PAGE)
	String page;
	public String getPage()	{
		return page;
	}

	public void setPage(String page) {
		this.page = page;
	}

	@PartitionKey(1)
	@CqlName(FIELD_ALIAS_GROUP)
	private String group;
	public String getGroup() {
		return group;
	}

	public void setGroup(String group) {
		this.group = group;
	}

	@ClusteringColumn(0)
	@CqlName(FIELD_FIRST_MESSAGE_DATE)
	private LocalDate firstMessageDate;
	public LocalDate getFirstMessageDate() {
		return firstMessageDate;
	}

	public void setFirstMessageDate(LocalDate messageDate) {
		this.firstMessageDate = messageDate;
	}

	@ClusteringColumn(1)
	@CqlName(FIELD_FIRST_MESSAGE_TIME)
	private LocalTime firstMessageTime;
	public LocalTime getFirstMessageTime() {
		return firstMessageTime;
	}

	public void setFirstMessageTime(LocalTime messageTime) {
		this.firstMessageTime = messageTime;
	}


	@CqlName(FIELD_LAST_MESSAGE_DATE)
	private LocalDate lastMessageDate;
	public LocalDate getLastMessageDate() {
		return lastMessageDate;
	}


	public void setLastMessageDate(LocalDate messageDate) {
		this.lastMessageDate = messageDate;
	}


	@CqlName(FIELD_LAST_MESSAGE_TIME)
	private LocalTime lastMessageTime;
	public LocalTime getLastMessageTime() {
		return lastMessageTime;
	}

	public void setLastMessageTime(LocalTime messageTime) {
		this.lastMessageTime = messageTime;
	}

	@CqlName(FIELD_MESSAGE_COUNT)
	private int messageCount;
	public int getMessageCount() {
		return messageCount;
	}

	public void setMessageCount(int messageCount) {
		this.messageCount = messageCount;
	}

	@CqlName(FIELD_REC_DATE)
	private Instant recDate;
	public Instant getRecDate() {
		return recDate;
	}

	public void setRecDate(Instant recDate) {
		this.recDate = recDate;
	}

	@Transient
	public void setFirstMessageTimestamp(Instant timestamp) {
		LocalDateTime ldt = TimeUtils.toLocalTimestamp(timestamp);
		setFirstMessageDate(ldt.toLocalDate());
		setFirstMessageTime(ldt.toLocalTime());
	}

	@Transient
	public void setLastMessageTimestamp(Instant timestamp) {
		LocalDateTime ldt = TimeUtils.toLocalTimestamp(timestamp);
		setLastMessageDate(ldt.toLocalDate());
		setLastMessageTime(ldt.toLocalTime());
	}

	private List<SerializedEntityMetadata> serializedMessageMetadata;
	@Transient
	public List<SerializedEntityMetadata> getSerializedMessageMetadata() {
		return serializedMessageMetadata;
	}

	@Transient
	public void setSerializedMessageMetadata(List<SerializedEntityMetadata> serializedMessageMetadata) {
		this.serializedMessageMetadata = serializedMessageMetadata;
	}

	public StoredGroupedMessageBatch toStoredGroupedMessageBatch(PageId pageId)	throws DataFormatException, IOException
	{
		logger.debug("Creating grouped message batch from entity");

		byte[] content = restoreContent(group);
		List<StoredMessage> storedMessages = MessageUtils.deserializeMessages(content, pageId.getBookId());
		return new StoredGroupedMessageBatch(group, storedMessages, pageId, recDate);
	}

	private byte[] restoreContent(String group) throws DataFormatException, IOException {
		ByteBuffer content = getContent();
		if (content == null)
			return null;

		byte[] result = content.array();
		if (isCompressed()) {
			logger.trace("Decompressing content of grouped message batch '{}'", group);
			return CompressionUtils.decompressData(result);
		}
		return result;
	}
}

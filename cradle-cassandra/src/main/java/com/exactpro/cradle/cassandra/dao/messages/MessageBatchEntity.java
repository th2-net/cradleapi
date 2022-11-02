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
import com.exactpro.cradle.cassandra.dao.CradleEntity;
import com.exactpro.cradle.messages.MessageBatch;
import com.exactpro.cradle.utils.TimeUtils;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Set;


/**
 * Contains all data about {@link MessageBatch} to store in Cassandra
 */
@Entity
@CqlName(MessageBatchEntity.TABLE_NAME)
@PropertyStrategy(mutable = false)
public class MessageBatchEntity extends CradleEntity
{
	public static final String TABLE_NAME = "messages";

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

	public MessageBatchEntity()
	{
	}

	public MessageBatchEntity(String book, String page, String sessionAlias, String direction, LocalDate firstMessageDate, LocalTime firstMessageTime, long sequence, LocalDate lastMessageDate, LocalTime lastMessageTime, int messageCount, long lastSequence, Instant recDate, boolean compressed, Set<String> labels, ByteBuffer content) {
		super(compressed, labels, content);

		this.book = book;
		this.page = page;
		this.sessionAlias = sessionAlias;
		this.direction = direction;
		this.firstMessageDate = firstMessageDate;
		this.firstMessageTime = firstMessageTime;
		this.sequence = sequence;
		this.lastMessageDate = lastMessageDate;
		this.lastMessageTime = lastMessageTime;
		this.messageCount = messageCount;
		this.lastSequence = lastSequence;
		this.recDate = recDate;
	}

	public String getBook()
	{
		return book;
	}

	public String getPage()
	{
		return page;
	}

	public LocalDate getFirstMessageDate()
	{
		return firstMessageDate;
	}

	public String getSessionAlias()
	{
		return sessionAlias;
	}

	public LocalTime getFirstMessageTime()
	{
		return firstMessageTime;
	}

	public long getSequence()
	{
		return sequence;
	}

	public String getDirection()
	{
		return direction;
	}

	public LocalDate getLastMessageDate()
	{
		return lastMessageDate;
	}

	public LocalTime getLastMessageTime()
	{
		return lastMessageTime;
	}

	public int getMessageCount()
	{
		return messageCount;
	}

	public long getLastSequence()
	{
		return lastSequence;
	}

	public Instant getRecDate() {
		return recDate;
	}

	public static class MessageBatchEntityBuilder extends CradleEntityBuilder<MessageBatchEntity> {

		private String book;
		private String page;
		private String sessionAlias;
		private String direction;
		private LocalDate firstMessageDate;
		private LocalTime firstMessageTime;
		private long sequence;
		private LocalDate lastMessageDate;
		private LocalTime lastMessageTime;
		private int messageCount;
		private long lastSequence;
		private Instant recDate;

		public MessageBatchEntityBuilder () {
		}

		public MessageBatchEntityBuilder setBook(String book) {
			this.book = book;
			return this;
		}

		public MessageBatchEntityBuilder setPage(String page) {
			this.page = page;
			return this;
		}

		public MessageBatchEntityBuilder setSessionAlias(String sessionAlias) {
			this.sessionAlias = sessionAlias;
			return this;
		}

		public MessageBatchEntityBuilder setDirection(String direction) {
			this.direction = direction;
			return this;
		}

		public MessageBatchEntityBuilder setFirstMessageDate(LocalDate firstMessageDate) {
			this.firstMessageDate = firstMessageDate;
			return this;
		}

		public MessageBatchEntityBuilder setFirstMessageTime(LocalTime firstMessageTime) {
			this.firstMessageTime = firstMessageTime;
			return this;
		}

		public MessageBatchEntityBuilder setSequence(long sequence) {
			this.sequence = sequence;
			return this;
		}

		public MessageBatchEntityBuilder setLastMessageDate(LocalDate lastMessageDate) {
			this.lastMessageDate = lastMessageDate;
			return this;
		}

		public MessageBatchEntityBuilder setLastMessageTime(LocalTime lastMessageTime) {
			this.lastMessageTime = lastMessageTime;
			return this;
		}

		public MessageBatchEntityBuilder setMessageCount(int messageCount) {
			this.messageCount = messageCount;
			return this;
		}

		public MessageBatchEntityBuilder setLastSequence(long lastSequence) {
			this.lastSequence = lastSequence;
			return this;
		}

		public MessageBatchEntityBuilder setRecDate(Instant recDate) {
			this.recDate = recDate;
			return this;
		}

		public MessageBatchEntityBuilder setCompressed(boolean compressed) {
			this.compressed = compressed;
			return this;
		}

		public MessageBatchEntityBuilder setLabels(Set<String> labels) {
			this.labels = labels;
			return this;
		}

		public MessageBatchEntityBuilder setContent(ByteBuffer content) {
			this.content = content;
			return this;
		}

		public MessageBatchEntityBuilder setLastMessageTimestamp(MessageBatchEntity.MessageBatchEntityBuilder builder, Instant timestamp)
		{
			LocalDateTime ldt = TimeUtils.toLocalTimestamp(timestamp);
			setLastMessageDate(ldt.toLocalDate());
			setLastMessageTime(ldt.toLocalTime());

			return this;
		}

		public MessageBatchEntityBuilder setFirstMessageTimestamp(MessageBatchEntity.MessageBatchEntityBuilder builder, Instant timestamp)
		{
			LocalDateTime ldt = TimeUtils.toLocalTimestamp(timestamp);
			setFirstMessageDate(ldt.toLocalDate());
			setFirstMessageTime(ldt.toLocalTime());

			return this;
		}

		public MessageBatchEntity build() {
			return new MessageBatchEntity(
					book,
					page,
					sessionAlias,
					direction,
					firstMessageDate,
					firstMessageTime,
					sequence,
					lastMessageDate,
					lastMessageTime,
					messageCount,
					lastSequence,
					recDate,
					compressed,
					labels,
					content);
		}

		public static MessageBatchEntityBuilder builder () {
			return new MessageBatchEntityBuilder();
		}
	}
}

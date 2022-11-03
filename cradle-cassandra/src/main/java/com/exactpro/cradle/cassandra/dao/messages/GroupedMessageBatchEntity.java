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

import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Set;

@Entity
@CqlName(GroupedMessageBatchEntity.TABLE_NAME)
@PropertyStrategy(mutable = false)
public final class GroupedMessageBatchEntity extends CradleEntity {
	public static final String TABLE_NAME = "grouped_messages";

	public static final String FIELD_BOOK = "book";
	public static final String FIELD_PAGE = "page";
	public static final String FIELD_ALIAS_GROUP = "alias_group";
	public static final String FIELD_FIRST_MESSAGE_DATE = "first_message_date";
	public static final String FIELD_FIRST_MESSAGE_TIME = "first_message_time";
	public static final String FIELD_LAST_MESSAGE_DATE = "last_message_date";
	public static final String FIELD_LAST_MESSAGE_TIME = "last_message_time";
	public static final String FIELD_MESSAGE_COUNT = "message_count";
	public static final String  FIELD_REC_DATE = "rec_date";

	@PartitionKey(1)
	@CqlName(FIELD_BOOK)
	String book;

	@PartitionKey(2)
	@CqlName(FIELD_PAGE)
	String page;

	@PartitionKey(3)
	@CqlName(FIELD_ALIAS_GROUP)
	private String group;

	@ClusteringColumn(1)
	@CqlName(FIELD_FIRST_MESSAGE_DATE)
	private LocalDate firstMessageDate;

	@ClusteringColumn(2)
	@CqlName(FIELD_FIRST_MESSAGE_TIME)
	private LocalTime firstMessageTime;

	@CqlName(FIELD_LAST_MESSAGE_DATE)
	private LocalDate lastMessageDate;

	@CqlName(FIELD_LAST_MESSAGE_TIME)
	private LocalTime lastMessageTime;

	@CqlName(FIELD_MESSAGE_COUNT)
	private int messageCount;

	@CqlName(FIELD_REC_DATE)
	private Instant recDate;

	public GroupedMessageBatchEntity() {
	}

	public GroupedMessageBatchEntity(String book,
									 String page,
									 String group,
									 LocalDate firstMessageDate,
									 LocalTime firstMessageTime,
									 LocalDate lastMessageDate,
									 LocalTime lastMessageTime,
									 int messageCount,
									 Instant recDate,
									 boolean compressed,
									 Set<String> labels,
									 ByteBuffer content)
	{
		super(compressed, labels, content);

		this.book = book;
		this.page = page;
		this.group = group;
		this.firstMessageDate = firstMessageDate;
		this.firstMessageTime = firstMessageTime;
		this.lastMessageDate = lastMessageDate;
		this.lastMessageTime = lastMessageTime;
		this.messageCount = messageCount;
		this.recDate = recDate;
	}

	private static GroupedMessageBatchEntity build(GroupedMessageBatchEntityBuilder builder) {
		return new GroupedMessageBatchEntity(
												builder.book,
												builder.page,
												builder.group,
												builder.firstMessageDate,
												builder.firstMessageTime,
												builder.lastMessageDate,
												builder.lastMessageTime,
												builder.messageCount,
												builder.recDate,
												builder.isCompressed(),
												builder.getLabels(),
												builder.getContent());
	}


	public String getBook()	{
		return book;
	}

	public String getPage()	{
		return page;
	}

	public String getGroup() {
		return group;
	}

	public LocalDate getFirstMessageDate() {
		return firstMessageDate;
	}

	public LocalTime getFirstMessageTime() {
		return firstMessageTime;
	}

	public LocalDate getLastMessageDate() {
		return lastMessageDate;
	}

	public LocalTime getLastMessageTime() {
		return lastMessageTime;
	}

	public int getMessageCount() {
		return messageCount;
	}

	public Instant getRecDate() {
		return recDate;
	}

	public static GroupedMessageBatchEntityBuilder builder() {
		return new GroupedMessageBatchEntityBuilder();
	}

	public static class GroupedMessageBatchEntityBuilder extends CradleEntityBuilder<GroupedMessageBatchEntity, GroupedMessageBatchEntityBuilder> {
		String book;
		String page;
		private String group;
		private LocalDate firstMessageDate;
		private LocalTime firstMessageTime;
		private LocalDate lastMessageDate;
		private LocalTime lastMessageTime;
		private int messageCount;
		private Instant recDate;

		private GroupedMessageBatchEntityBuilder () {
		}

		public GroupedMessageBatchEntityBuilder setBook (String book) {
			this.book = book;
			return this;
		}

		public GroupedMessageBatchEntityBuilder setPage (String page) {
			this.page = page;
			return this;
		}

		public GroupedMessageBatchEntityBuilder setGroup (String group) {
			this.group = group;
			return this;
		}

		public GroupedMessageBatchEntityBuilder setFirstMessageDate (LocalDate firstMessageDate) {
			this.firstMessageDate = firstMessageDate;
			return this;
		}

		public GroupedMessageBatchEntityBuilder setFirstMessageTime (LocalTime firstMessageTime) {
			this.firstMessageTime = firstMessageTime;
			return this;
		}

		public GroupedMessageBatchEntityBuilder setLastMessageDate (LocalDate lastMessageDate) {
			this.lastMessageDate = lastMessageDate;
			return this;
		}

		public GroupedMessageBatchEntityBuilder setLastMessageTime (LocalTime lastMessageTime) {
			this.lastMessageTime = lastMessageTime;
			return this;
		}

		public GroupedMessageBatchEntityBuilder setMessageCount (int messageCount) {
			this.messageCount = messageCount;
			return this;
		}

		public GroupedMessageBatchEntityBuilder setRecDate (Instant recDate) {
			this.recDate = recDate;
			return this;
		}

		public GroupedMessageBatchEntityBuilder setCompressed (boolean compressed) {
			super.setCompressed(compressed);
			return this;
		}

		public GroupedMessageBatchEntityBuilder setLabels (Set<String> labels) {
			super.setLabels(labels);
			return this;
		}

		public GroupedMessageBatchEntityBuilder setContent (ByteBuffer content) {
			super.setContent(content);
			return this;
		}

		public GroupedMessageBatchEntity build () {
			return GroupedMessageBatchEntity.build(this);
		}
	}
}

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

package com.exactpro.cradle.cassandra.dao.testevents;

import com.datastax.oss.driver.api.mapper.annotations.*;
import com.exactpro.cradle.cassandra.dao.CradleEntity;
import com.exactpro.cradle.utils.TimeUtils;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Set;

@Entity
@CqlName(TestEventEntity.TABLE_NAME)
@PropertyStrategy(mutable = false)
public final class TestEventEntity extends CradleEntity {
	public static final String TABLE_NAME = "test_events";

	public static final String FIELD_BOOK = "book";
	public static final String FIELD_PAGE = "page";
	public static final String FIELD_SCOPE = "scope";
	public static final String FIELD_START_DATE = "start_date";
	public static final String FIELD_START_TIME = "start_time";
	public static final String FIELD_ID = "id";
	public static final String FIELD_NAME = "name";
	public static final String FIELD_TYPE = "type";
	public static final String FIELD_SUCCESS = "success";
	public static final String FIELD_ROOT = "root";
	public static final String FIELD_PARENT_ID = "parent_id";
	public static final String FIELD_EVENT_BATCH = "event_batch";
	public static final String FIELD_EVENT_COUNT = "event_count";
	public static final String FIELD_END_DATE = "end_date";
	public static final String FIELD_END_TIME = "end_time";
	public static final String FIELD_MESSAGES = "messages";
	public static final String FIELD_REC_DATE = "rec_date";

	@PartitionKey(1)
	@CqlName(FIELD_BOOK)
	private String book;

	@PartitionKey(2)
	@CqlName(FIELD_PAGE)
	private String page;

	@PartitionKey(3)
	@CqlName(FIELD_SCOPE)
	private String scope;
	
	@ClusteringColumn(1)
	@CqlName(FIELD_START_DATE)
	private LocalDate startDate;
	
	@ClusteringColumn(2)
	@CqlName(FIELD_START_TIME)
	private LocalTime startTime;
	
	@ClusteringColumn(3)
	@CqlName(FIELD_ID)
	private String id;
	
	@CqlName(FIELD_NAME)
	private String name;
	
	@CqlName(FIELD_TYPE)
	private String type;
	
	@CqlName(FIELD_SUCCESS)
	private boolean success;
	
	@CqlName(FIELD_ROOT)
	private boolean root;
	
	@CqlName(FIELD_PARENT_ID)
	private String parentId;

	@CqlName(FIELD_EVENT_BATCH)
	private boolean eventBatch;
	
	@CqlName(FIELD_EVENT_COUNT)
	private int eventCount;
	
	@CqlName(FIELD_END_DATE)
	private LocalDate endDate;
	@CqlName(FIELD_END_TIME)
	private LocalTime endTime;
	
	@CqlName(FIELD_MESSAGES)
	private ByteBuffer messages;

	@CqlName(FIELD_REC_DATE)
	private Instant recDate;

	public TestEventEntity() {
	}

	public TestEventEntity(String book,
						   String page,
						   String scope,
						   LocalDate startDate,
						   LocalTime startTime,
						   String id,
						   String name,
						   String type,
						   boolean success,
						   boolean root,
						   String parentId,
						   boolean eventBatch,
						   int eventCount,
						   LocalDate endDate,
						   LocalTime endTime,
						   Instant recDate,
						   ByteBuffer messages,
						   boolean compressed,
						   Set<String> labels, ByteBuffer content)
	{
		super(compressed, labels, content);

		this.book = book;
		this.page = page;
		this.scope = scope;
		this.startDate = startDate;
		this.startTime = startTime;
		this.id = id;
		this.name = name;
		this.type = type;
		this.success = success;
		this.root = root;
		this.parentId = parentId;
		this.eventBatch = eventBatch;
		this.eventCount = eventCount;
		this.endDate = endDate;
		this.endTime = endTime;
		this.messages = messages;
		this.recDate = recDate;
	}

	private static TestEventEntity build(TestEventEntityBuilder builder) {
		return new TestEventEntity(
									builder.book,
									builder.page,
									builder.scope,
									builder.startDate,
									builder.startTime,
									builder.id,
									builder.name,
									builder.type,
									builder.success,
									builder.root,
									builder.parentId,
									builder.eventBatch,
									builder.eventCount,
									builder.endDate,
									builder.endTime,
									builder.recDate,
									builder.messages,
									builder.isCompressed(),
									builder.getLabels(),
									builder.getContent());
	}

	public String getBook()
	{
		return book;
	}

	public String getPage()
	{
		return page;
	}
	
	public String getScope()
	{
		return scope;
	}
	
	public LocalDate getStartDate()
	{
		return startDate;
	}

	public LocalTime getStartTime()
	{
		return startTime;
	}
	
	public String getId()
	{
		return id;
	}
	
	public String getName()
	{
		return name;
	}
	
	public String getType()
	{
		return type;
	}

	public boolean isSuccess()
	{
		return success;
	}
	
	public boolean isRoot()
	{
		return root;
	}

	public String getParentId()
	{
		return parentId;
	}

	public boolean isEventBatch()
	{
		return eventBatch;
	}

	public int getEventCount()
	{
		return eventCount;
	}

	public LocalDate getEndDate()
	{
		return endDate;
	}

	public LocalTime getEndTime()
	{
		return endTime;
	}

	public Instant getRecDate() {
		return recDate;
	}

	public ByteBuffer getMessages()
	{
		return messages;
	}

	public static TestEventEntityBuilder builder () {
		return new TestEventEntityBuilder();
	}

	public static class TestEventEntityBuilder extends CradleEntityBuilder<TestEventEntity, TestEventEntityBuilder> {
		private String book;
		private String page;
		private String scope;
		private LocalDate startDate;
		private LocalTime startTime;
		private String id;
		private String name;
		private String type;
		private boolean success;
		private boolean root;
		private String parentId;
		private boolean eventBatch;
		private int eventCount;
		private LocalDate endDate;
		private LocalTime endTime;
		private ByteBuffer messages;
		private Instant recDate;

		private  TestEventEntityBuilder () {
		}
	
		public TestEventEntityBuilder setBook(String book) {
			this.book = book;
			return this;
		}
	
		public TestEventEntityBuilder setPage(String page) {
			this.page = page;
			return this;
		}
	
		public TestEventEntityBuilder setScope(String scope) {
			this.scope = scope;
			return this;
		}
	
		public TestEventEntityBuilder setStartDate(LocalDate startDate) {
			this.startDate = startDate;
			return this;
		}
	
		public TestEventEntityBuilder setStartTime(LocalTime startTime) {
			this.startTime = startTime;
			return this;
		}
	
		public TestEventEntityBuilder setId(String id) {
			this.id = id;
			return this;
		}
	
		public TestEventEntityBuilder setName(String name) {
			this.name = name;
			return this;
		}
	
		public TestEventEntityBuilder setType(String type) {
			this.type = type;
			return this;
		}
	
		public TestEventEntityBuilder setSuccess(boolean success) {
			this.success = success;
			return this;
		}
	
		public TestEventEntityBuilder setRoot(boolean root) {
			this.root = root;
			return this;
		}
	
		public TestEventEntityBuilder setParentId(String parentId) {
			this.parentId = parentId;
			return this;
		}
	
		public TestEventEntityBuilder setEventBatch(boolean eventBatch) {
			this.eventBatch = eventBatch;
			return this;
		}
	
		public TestEventEntityBuilder setEventCount(int eventCount) {
			this.eventCount = eventCount;
			return this;
		}
	
		public TestEventEntityBuilder setEndDate(LocalDate endDate) {
			this.endDate = endDate;
			return this;
		}
	
		public TestEventEntityBuilder setEndTime(LocalTime endTime) {
			this.endTime = endTime;
			return this;
		}
	
		public TestEventEntityBuilder setMessages(ByteBuffer messages) {
			this.messages = messages;
			return this;
		}
	
		public TestEventEntityBuilder setRecDate(Instant recDate) {
			this.recDate = recDate;
			return this;
		}
	
		public TestEventEntityBuilder setStartTimestamp(Instant timestamp) {
			if (timestamp != null) {
				setStartTimestamp(TimeUtils.toLocalTimestamp(timestamp));
			} else {
				setStartDate(null);
				setStartTime(null);
			}
	
			return this;
		}
	
		public TestEventEntityBuilder setStartTimestamp(LocalDateTime timestamp) {
			if (timestamp != null) {
				setStartDate(timestamp.toLocalDate());
				setStartTime(timestamp.toLocalTime());
			} else {
				setStartDate(null);
				setStartTime(null);
			}
	
			return this;
		}
	
		public TestEventEntityBuilder setEndTimestamp(Instant timestamp) {
			if (timestamp != null) {
				LocalDateTime ldt = TimeUtils.toLocalTimestamp(timestamp);
				setEndDate(ldt.toLocalDate());
				setEndTime(ldt.toLocalTime());
			} else {
				setEndDate(null);
				setEndTime(null);
			}

			return this;
		}
	

		public TestEventEntity build () {
			return TestEventEntity.build(this);
		}
	}
}

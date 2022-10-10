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
import com.exactpro.cradle.BookId;
import com.exactpro.cradle.PageId;
import com.exactpro.cradle.cassandra.dao.CradleEntity;
import com.exactpro.cradle.messages.StoredMessageId;
import com.exactpro.cradle.serialization.SerializedEntityData;
import com.exactpro.cradle.serialization.SerializedEntityMetadata;
import com.exactpro.cradle.testevents.*;
import com.exactpro.cradle.utils.*;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.DataFormatException;

/**
 * Contains data of {@link StoredTestEvent} to write to or to obtain from Cassandra
 */
@Entity
@CqlName(TestEventEntity.TABLE_NAME)
public class TestEventEntity extends CradleEntity {
	public static final String TABLE_NAME = "test_events";
	private static final Logger logger = LoggerFactory.getLogger(TestEventEntity.class);

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

	private List<SerializedEntityMetadata> serializedEventMetadata;

	public TestEventEntity() {
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
	
	
	public String getScope()
	{
		return scope;
	}
	
	public void setScope(String scope)
	{
		this.scope = scope;
	}
	
	
	public LocalDate getStartDate()
	{
		return startDate;
	}
	
	public void setStartDate(LocalDate startDate)
	{
		this.startDate = startDate;
	}	
	
	public LocalTime getStartTime()
	{
		return startTime;
	}
	
	public void setStartTime(LocalTime startTime)
	{
		this.startTime = startTime;
	}
	
	@Transient
	public Instant getStartTimestamp()
	{
		return TimeUtils.toInstant(getStartDate(), getStartTime());
	}
	
	public String getId()
	{
		return id;
	}
	
	public void setId(String id)
	{
		this.id = id;
	}
	
	
	public String getName()
	{
		return name;
	}
	
	public void setName(String name)
	{
		this.name = name;
	}
	
	
	public String getType()
	{
		return type;
	}
	
	public void setType(String type)
	{
		this.type = type;
	}
	
	
	public boolean isSuccess()
	{
		return success;
	}
	
	public void setSuccess(boolean success)
	{
		this.success = success;
	}
	

	public boolean isRoot()
	{
		return root;
	}
	
	public void setRoot(boolean root)
	{
		this.root = root;
	}
	

	public String getParentId()
	{
		return parentId;
	}
	
	public void setParentId(String parentId)
	{
		this.parentId = parentId;
	}
	
	
	public boolean isEventBatch()
	{
		return eventBatch;
	}
	
	public void setEventBatch(boolean eventBatch)
	{
		this.eventBatch = eventBatch;
	}
	
	
	public int getEventCount()
	{
		return eventCount;
	}
	
	public void setEventCount(int eventCount)
	{
		this.eventCount = eventCount;
	}
	
	
	public LocalDate getEndDate()
	{
		return endDate;
	}
	
	public void setEndDate(LocalDate endDate)
	{
		this.endDate = endDate;
	}
	
	public LocalTime getEndTime()
	{
		return endTime;
	}
	
	public void setEndTime(LocalTime endTime)
	{
		this.endTime = endTime;
	}

	@Transient
	public Instant getEndTimestamp()
	{
		return TimeUtils.toInstant(getEndDate(), getEndTime());
	}

	public Instant getRecDate() {
		return recDate;
	}

	public void setRecDate(Instant recDate) {
		this.recDate = recDate;
	}

	@Transient
	public List<SerializedEntityMetadata> getSerializedEventMetadata() {
		return serializedEventMetadata;
	}

	@Transient
	public void setSerializedEventMetadata(List<SerializedEntityMetadata> serializedEventMetadata) {
		this.serializedEventMetadata = serializedEventMetadata;
	}

	public ByteBuffer getMessages()
	{
		return messages;
	}
	
	public void setMessages(ByteBuffer messages)
	{
		this.messages = messages;
	}
	
	
	public StoredTestEvent toStoredTestEvent(PageId pageId) 
			throws IOException, CradleStorageException, DataFormatException, CradleIdException
	{
		StoredTestEventId eventId = createId(pageId.getBookId());
		logger.trace("Creating test event '{}' from entity", eventId);
		
		byte[] content = restoreContent(eventId);
		return isEventBatch() ? toStoredTestEventBatch(pageId, eventId, content) : toStoredTestEventSingle(pageId, eventId, content);
	}
	
	
	private StoredTestEventId createId(BookId bookId)
	{
		return new StoredTestEventId(bookId, getScope(), getStartTimestamp(), getId());
	}
	
	private StoredTestEventId createParentId() throws CradleIdException
	{
		return StringUtils.isEmpty(getParentId()) ? null : StoredTestEventId.fromString(getParentId());
	}
	
	
	private byte[] restoreContent(StoredTestEventId eventId) throws IOException, DataFormatException
	{
		ByteBuffer content = getContent();
		if (content == null)
			return null;
		
		byte[] result = content.array();
		if (isCompressed())
		{
			logger.trace("Decompressing content of test event '{}'", eventId);
			return CompressionUtils.decompressData(result);
		}
		return result;
	}
	
	private Set<StoredMessageId> restoreMessages(BookId bookId) 
			throws IOException, DataFormatException, CradleIdException
	{
		ByteBuffer messages = getMessages();
		if (messages == null)
			return null;
		
		byte[] result = messages.array();
		return TestEventUtils.deserializeLinkedMessageIds(result, bookId);
	}
	
	private Map<StoredTestEventId, Set<StoredMessageId>> restoreBatchMessages(BookId bookId) 
			throws IOException, DataFormatException, CradleIdException
	{
		ByteBuffer messages = getMessages();
		if (messages == null)
			return null;
		
		byte[] result = messages.array();
		return TestEventUtils.deserializeBatchLinkedMessageIds(result, bookId);
	}
	
	
	private StoredTestEventSingle toStoredTestEventSingle(PageId pageId, StoredTestEventId eventId, byte[] content) 
			throws IOException, CradleStorageException, DataFormatException, CradleIdException
	{
		Set<StoredMessageId> messages = restoreMessages(pageId.getBookId());
		return new StoredTestEventSingle(eventId, getName(), getType(), createParentId(),
				getEndTimestamp(), isSuccess(), content, messages, pageId, null, recDate);
	}
	
	private StoredTestEventBatch toStoredTestEventBatch(PageId pageId, StoredTestEventId eventId, byte[] content) 
			throws IOException, CradleStorageException, DataFormatException, CradleIdException
	{
		Collection<BatchedStoredTestEvent> children = TestEventUtils.deserializeTestEvents(content, eventId);
		Map<StoredTestEventId, Set<StoredMessageId>> messages = restoreBatchMessages(pageId.getBookId());
		return new StoredTestEventBatch(eventId, getName(), getType(), createParentId(),
				children, messages, pageId, null, recDate);
	}
}

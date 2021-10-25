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

package com.exactpro.cradle.cassandra.dao.testevents;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.DataFormatException;

import com.exactpro.cradle.cassandra.dao.BlobChunkData;
import com.exactpro.cradle.cassandra.dao.EntityUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.oss.driver.api.core.MappedAsyncPagingIterable;
import com.exactpro.cradle.BookId;
import com.exactpro.cradle.PageId;
import com.exactpro.cradle.messages.StoredMessageId;
import com.exactpro.cradle.testevents.BatchedStoredTestEvent;
import com.exactpro.cradle.testevents.StoredTestEvent;
import com.exactpro.cradle.testevents.StoredTestEventBatch;
import com.exactpro.cradle.testevents.StoredTestEventId;
import com.exactpro.cradle.testevents.StoredTestEventSingle;
import com.exactpro.cradle.testevents.TestEventToStore;
import com.exactpro.cradle.utils.CompressionUtils;
import com.exactpro.cradle.utils.CradleIdException;
import com.exactpro.cradle.utils.CradleStorageException;
import com.exactpro.cradle.utils.TestEventUtils;

public class EventEntityUtils
{
	private static final Logger logger = LoggerFactory.getLogger(EventEntityUtils.class);
	
	public static boolean isMultiChunk(byte[] content, Set<String> messages, int maxChunkSize, int maxMessageCount)
	{
		return content.length > maxChunkSize || (messages != null && messages.size() > maxMessageCount);
	}
	
	public static List<TestEventEntity> toEntities(TestEventToStore event, PageId pageId, int maxUncompressedSize, 
			int contentChunkSize, int messagesChunkSize) throws IOException
	{
		byte[] content = TestEventUtils.getTestEventContent(event);
		boolean compressed;
		if (content != null && content.length > maxUncompressedSize)
		{
			logger.trace("Compressing content of test event '{}'", event.getId());
			content = CompressionUtils.compressData(content);
			compressed = true;
		}
		else
			compressed = false;
		
		byte[] messages = TestEventUtils.serializeLinkedMessageIds(event);
		
		return toEntities(event, pageId, content, compressed, messages, contentChunkSize, messagesChunkSize);
	}
	
	public static List<TestEventEntity> toEntities(TestEventToStore event, PageId pageId, byte[] content, boolean compressed,
			byte[] messages, int contentChunkSize, int messagesChunkSize)
	{
		List<TestEventEntity> result = new ArrayList<>();
		int contentPos = 0,
				messagesPos = 0;
		logger.debug("Creating chunks from test event '{}'", event.getId());
		boolean last = false;
		do
		{
			BlobChunkData contentChunk = EntityUtils.nextChunk(content, contentPos, contentChunkSize),
					messagesChunk = EntityUtils.nextChunk(messages, messagesPos, messagesChunkSize);
			byte[] entityContent;
			if (contentChunk != null)
			{
				entityContent = contentChunk.getChunk();
				contentPos = contentChunk.getPosition();
			}
			else
				entityContent = null;
			
			byte[] entityMessages;
			if (messagesChunk != null)
			{
				entityMessages = messagesChunk.getChunk();
				messagesPos = messagesChunk.getPosition();
			}
			else
				entityMessages = null;
			
			last = (entityContent == null || contentPos >= content.length-1) && (entityMessages == null || messagesPos >= messages.length-1);
			if (logger.isDebugEnabled())
				logger.debug("Creating chunk #{}{} from test event '{}'", 
						result.size()+1, 
						last ? " (last one)" : "", 
						event.getId());
			TestEventEntity entity = new TestEventEntity(event, pageId, result.size(), last, 
					entityContent, compressed, entityMessages);
			result.add(entity);
		}
		while (!last);
		return result;
	}
	
	
	public static StoredTestEvent toStoredTestEvent(List<TestEventEntity> entities, PageId pageId) 
			throws IOException, CradleStorageException, DataFormatException, CradleIdException
	{
		logger.debug("Creating test event from {} chunk(s)", entities.size());
		if (entities.isEmpty())
			return null;
		
		TestEventEntity firstEntity = entities.get(0);
		String error = EntityUtils.validateEntities(entities);
		if (error != null)
			return toErrorEvent(firstEntity, pageId, error);
		return firstEntity.isEventBatch() ? toStoredTestEventBatch(entities, pageId) : toStoredTestEventSingle(entities, pageId);
	}
	
	public static StoredTestEvent toStoredTestEvent(MappedAsyncPagingIterable<TestEventEntity> resultSet, PageId pageId) 
			throws IOException, CradleStorageException, DataFormatException, CradleIdException
	{
		List<TestEventEntity> entities = EntityUtils.toCompleteEntitiesCollection(resultSet);
		
		return toStoredTestEvent(entities, pageId);
	}
	
	
	private static StoredTestEventId createId(TestEventEntity entity, BookId bookId)
	{
		return new StoredTestEventId(bookId, entity.getScope(), entity.getStartTimestamp(), entity.getId());
	}
	
	private static StoredTestEventId createParentId(TestEventEntity entity) throws CradleIdException
	{
		return StringUtils.isEmpty(entity.getParentId()) ? null : StoredTestEventId.fromString(entity.getParentId());
	}
	
	private static byte[] getContent(Collection<TestEventEntity> entities, StoredTestEventId eventId) throws IOException, DataFormatException
	{
		byte[] result = EntityUtils.uniteChunks(entities, e -> e.getContent());
		TestEventEntity entity = entities.iterator().next();
		if (entity.isCompressed())
		{
			logger.trace("Decompressing content of test event '{}'", eventId);
			return CompressionUtils.decompressData(result);
		}
		return result;
	}
	
	private static Set<StoredMessageId> getMessages(Collection<TestEventEntity> entities, BookId bookId) 
			throws IOException, DataFormatException, CradleIdException
	{
		byte[] result = EntityUtils.uniteChunks(entities, e -> e.getMessages());
		return TestEventUtils.deserializeLinkedMessageIds(result, bookId);
	}
	
	private static Map<StoredTestEventId, Set<StoredMessageId>> getBatchMessages(Collection<TestEventEntity> entities, BookId bookId) 
			throws IOException, DataFormatException, CradleIdException
	{
		byte[] result = EntityUtils.uniteChunks(entities, e -> e.getMessages());
		return TestEventUtils.deserializeBatchLinkedMessageIds(result, bookId);
	}
	
	private static StoredTestEventSingle toStoredTestEventSingle(Collection<TestEventEntity> entities, PageId pageId) 
			throws IOException, CradleStorageException, DataFormatException, CradleIdException
	{
		TestEventEntity entity = entities.iterator().next();
		BookId bookId = pageId.getBookId();
		
		StoredTestEventId eventId = createId(entity, bookId);
		byte[] eventContent = getContent(entities, eventId);
		Set<StoredMessageId> messages = getMessages(entities, bookId);
		return new StoredTestEventSingle(eventId, entity.getName(), entity.getType(), createParentId(entity),
				entity.getEndTimestamp(), entity.isSuccess(), eventContent, messages, pageId, null);
	}
	
	private static StoredTestEventBatch toStoredTestEventBatch(Collection<TestEventEntity> entities, PageId pageId) 
			throws IOException, CradleStorageException, DataFormatException, CradleIdException
	{
		TestEventEntity entity = entities.iterator().next();
		BookId bookId = pageId.getBookId();
		
		StoredTestEventId eventId = createId(entity, pageId.getBookId());
		byte[] eventContent = getContent(entities, eventId);
		Collection<BatchedStoredTestEvent> children = TestEventUtils.deserializeTestEvents(eventContent);
		Map<StoredTestEventId, Set<StoredMessageId>> messages = getBatchMessages(entities, bookId);
		return new StoredTestEventBatch(eventId, entity.getName(), entity.getType(), createParentId(entity),
				children, messages, pageId, null);
	}
	
	private static StoredTestEvent toErrorEvent(TestEventEntity entity, PageId pageId, String error) throws CradleIdException, CradleStorageException
	{
		StoredTestEventId id = createId(entity, pageId.getBookId()),
				parentId = createParentId(entity);
		logger.warn("Event '{}' is corrupted: {}", id, error);
		return entity.isEventBatch() 
				? new StoredTestEventBatch(id, entity.getName(), entity.getType(), parentId, null, null, pageId, error)
				: new StoredTestEventSingle(id, entity.getName(), entity.getType(), parentId,
						entity.getEndTimestamp(), entity.isSuccess(), null, null, pageId, error);
	}
}

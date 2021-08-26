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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.zip.DataFormatException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.oss.driver.api.core.MappedAsyncPagingIterable;
import com.exactpro.cradle.BookId;
import com.exactpro.cradle.PageId;
import com.exactpro.cradle.cassandra.dao.DaoUtils;
import com.exactpro.cradle.messages.StoredMessageId;
import com.exactpro.cradle.testevents.StoredTestEvent;
import com.exactpro.cradle.testevents.StoredTestEventBatch;
import com.exactpro.cradle.testevents.StoredTestEventId;
import com.exactpro.cradle.testevents.StoredTestEventSingle;
import com.exactpro.cradle.testevents.TestEventBatchToStoreBuilder;
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
	
	public static Collection<TestEventEntity> toEntities(StoredTestEvent event, PageId pageId, int maxUncompressedSize, 
			int contentChunkSize, int messagesPerChunk) throws IOException
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
		
		return toEntities(event, pageId, content, compressed, contentChunkSize, messagesPerChunk);
	}
	
	public static Collection<TestEventEntity> toEntities(StoredTestEvent event, PageId pageId, byte[] content, boolean compressed, 
			int contentChunkSize, int messagesPerChunk)
	{
		Set<StoredMessageId> eventMessages = event.getMessages();
		List<StoredMessageId> messages = eventMessages != null && eventMessages.size() > 0 ? new ArrayList<>(eventMessages) : null;
		Collection<TestEventEntity> result = new ArrayList<>();
		int contentPos = 0,
				messagesPos = 0;
		boolean last = false;
		do
		{
			byte[] entityContent;
			if (content != null)
			{
				int entityContentSize = Math.min(content.length-contentPos, contentChunkSize);
				if (entityContentSize > 0)  //Will be <=0 if the whole content is already stored, but messages are not
				{
					entityContent = Arrays.copyOfRange(content, contentPos, contentPos+entityContentSize);
					contentPos += entityContentSize;
				}
				else
					entityContent = null;
			}
			else
				entityContent = null;
			
			Set<String> entityMessages;
			if (messages != null)
			{
				int entityMessagesSize = Math.min(messages.size()-messagesPos, messagesPerChunk);
				if (entityMessagesSize > 0)  //Will be <=0 if all messages are already stored, but content is not
				{
					entityMessages = new HashSet<String>(entityMessagesSize);
					for (StoredMessageId id : messages.subList(messagesPos, messagesPos+entityMessagesSize))
						entityMessages.add(id.toString());
					messagesPos += entityMessagesSize;
				}
				else
					entityMessages = null;
			}
			else
				entityMessages = null;
			
			last = (entityContent == null || contentPos >= content.length-1) && (entityMessages == null || messagesPos >= messages.size()-1);
			TestEventEntity entity = new TestEventEntity(new EventEntityData(event, pageId, result.size(), last, 
					entityContent, compressed, entityMessages));
			result.add(entity);
		}
		while (!last);
		return result;
	}
	
	
	public static StoredTestEvent toStoredTestEvent(Collection<TestEventEntity> entities, BookId bookId) 
			throws IOException, CradleStorageException, DataFormatException, CradleIdException
	{
		Iterator<TestEventEntity> it = entities.iterator();
		if (!it.hasNext())
			return null;
		TestEventEntity entity = it.next();
		return entity.isEventBatch() ? toStoredTestEventBatch(entities, bookId) : toStoredTestEventSingle(entities, bookId);
	}
	
	public static StoredTestEvent toStoredTestEvent(MappedAsyncPagingIterable<TestEventEntity> resultSet, BookId bookId) 
			throws IOException, CradleStorageException, DataFormatException, CradleIdException
	{
		Collection<TestEventEntity> entities;
		try
		{
			entities = DaoUtils.toCollection(resultSet);
		}
		catch (Exception e)
		{
			throw new CradleStorageException("Error while converting result to collection", e);
		}
		return toStoredTestEvent(entities, bookId);
	}
	
	
	private static StoredTestEventId createId(TestEventEntity entity, BookId bookId)
	{
		return new StoredTestEventId(bookId, entity.getScope(), entity.getStartTimestamp(), entity.getId());
	}
	
	private static StoredTestEventId createParentId(TestEventEntity entity) throws CradleIdException
	{
		return entity.getParentId() != null ? StoredTestEventId.fromString(entity.getParentId()) : null;
	}
	
	private static byte[] uniteContents(Collection<TestEventEntity> entities)
	{
		int size = entities.stream()
				.mapToInt(e -> e.getContent() != null ? e.getContent().limit() : 0)
				.sum();
		
		if (size == 0)
			return null;
		
		ByteBuffer buffer = ByteBuffer.allocate(size);
		for (TestEventEntity e : entities)
		{
			if (e.getContent() == null)
				continue;
			buffer.put(e.getContent());
		}
		return buffer.array();
	}
	
	private static byte[] getContent(Collection<TestEventEntity> entities, StoredTestEventId eventId) throws IOException, DataFormatException
	{
		byte[] result = uniteContents(entities);
		TestEventEntity entity = entities.iterator().next();
		if (entity.isCompressed())
		{
			logger.trace("Decompressing content of test event '{}'", eventId);
			return CompressionUtils.decompressData(result);
		}
		return result;
	}
	
	private static Set<StoredMessageId> getMessages(Collection<TestEventEntity> entities) throws IOException, DataFormatException, CradleIdException
	{
		Set<StoredMessageId> result = null;
		for (TestEventEntity e : entities)
		{
			Set<String> messages = e.getMessages();
			if (messages == null)
				continue;
			
			if (result == null)
				result = new HashSet<>();
			for (String id : messages)
				result.add(StoredMessageId.fromString(id));
		}
		return result;
	}
	
	private static StoredTestEventSingle toStoredTestEventSingle(Collection<TestEventEntity> entities, BookId bookId) 
			throws IOException, CradleStorageException, DataFormatException, CradleIdException
	{
		TestEventEntity entity = entities.iterator().next();
		
		StoredTestEventId eventId = createId(entity, bookId);
		byte[] eventContent = getContent(entities, eventId);
		Set<StoredMessageId> messages = getMessages(entities);
		return StoredTestEvent.single(StoredTestEvent.singleBuilder()
				.id(eventId)
				.name(entity.getName())
				.type(entity.getType())
				.parentId(createParentId(entity))
				.endTimestamp(entity.getEndTimestamp())
				.success(entity.isSuccess())
				.messages(messages)
				.content(eventContent)
				.build());
	}
	
	private static StoredTestEventBatch toStoredTestEventBatch(Collection<TestEventEntity> entities, BookId bookId) 
			throws IOException, CradleStorageException, DataFormatException, CradleIdException
	{
		TestEventEntity entity = entities.iterator().next();
		
		StoredTestEventId eventId = createId(entity, bookId);
		byte[] eventContent = getContent(entities, eventId);
		//Test event batch doesn't contain messages, they are got from child events. In Cassandra messages are stored for batch to build index
		StoredTestEventBatch result = new StoredTestEventBatch(new TestEventBatchToStoreBuilder()
				.id(eventId)
				.name(entity.getName())
				.type(entity.getType())
				.parentId(createParentId(entity))
				.build());
		TestEventUtils.deserializeTestEvents(eventContent, result);
		return result;
	}
}
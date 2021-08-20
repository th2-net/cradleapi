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
import java.util.Set;
import java.util.zip.DataFormatException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.cradle.BookId;
import com.exactpro.cradle.PageId;
import com.exactpro.cradle.testevents.StoredTestEvent;
import com.exactpro.cradle.testevents.StoredTestEventBatch;
import com.exactpro.cradle.testevents.StoredTestEventId;
import com.exactpro.cradle.testevents.StoredTestEventSingle;
import com.exactpro.cradle.testevents.TestEventBatchToStoreBuilder;
import com.exactpro.cradle.testevents.TestEventSingleToStoreBuilder;
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
	
	public static Collection<TestEventEntity> toEntities(StoredTestEvent event, PageId pageId, int maxUncompressedSize, int chunkSize) throws IOException
	{
		byte[] content = TestEventUtils.getTestEventContent(event);
		boolean compressed;
		if (content.length > maxUncompressedSize)
		{
			logger.trace("Compressing content of test event '{}'", event.getId());
			content = CompressionUtils.compressData(content);
			compressed = true;
		}
		else
			compressed = false;
		
		return toEntities(event, pageId, content, compressed, chunkSize);
	}
	
	public static Collection<TestEventEntity> toEntities(StoredTestEvent event, PageId pageId, byte[] content, boolean compressed, int chunkSize)
	{
		//TODO: Set<String> messages = event.getMessages();
		Collection<TestEventEntity> result = new ArrayList<>();
		int contentPos = 0;
		boolean last = false;
		do
		{
			int entityContentSize = Math.min(content.length-contentPos, chunkSize);
			byte[] entityContent = Arrays.copyOfRange(content, contentPos, contentPos+entityContentSize);
			contentPos += entityContentSize;
			last = contentPos >= content.length-1;
			TestEventEntity entity = new TestEventEntity(new EventEntityData(event, pageId, result.size(), last, 
					entityContent, compressed, null));  //TODO: messages
			result.add(entity);
		}
		while (!last);
		return result;
	}
	
	
	public static StoredTestEventSingle toStoredTestEventSingle(Collection<TestEventEntity> entities, BookId bookId) 
			throws IOException, CradleStorageException, DataFormatException, CradleIdException
	{
		TestEventEntity entity = entities.iterator().next();
		
		StoredTestEventId eventId = createId(entity, bookId);
		byte[] eventContent = getContent(entities, eventId);
		return new StoredTestEventSingle(new TestEventSingleToStoreBuilder()
				.id(eventId)
				.name(entity.getName())
				.type(entity.getType())
				.parentId(createParentId(entity))
				.endTimestamp(entity.getEndTimestamp())
				.success(entity.isSuccess())
				//TODO: .setMessages()
				.content(eventContent)
				.build());
	}
	
	public static StoredTestEventBatch toStoredTestEventBatch(Collection<TestEventEntity> entities, BookId bookId) 
			throws IOException, CradleStorageException, DataFormatException, CradleIdException
	{
		TestEventEntity entity = entities.iterator().next();
		
		StoredTestEventId eventId = createId(entity, bookId);
		byte[] eventContent = getContent(entities, eventId);
		StoredTestEventBatch result = new StoredTestEventBatch(new TestEventBatchToStoreBuilder()
				.id(eventId)
				.name(entity.getName())
				.type(entity.getType())
				.parentId(createParentId(entity))
				.build());
		TestEventUtils.deserializeTestEvents(eventContent, result);
		return result;
	}
	
	public static StoredTestEvent toStoredTestEvent(Collection<TestEventEntity> entities, BookId bookId) 
			throws IOException, CradleStorageException, DataFormatException, CradleIdException
	{
		TestEventEntity entity = entities.iterator().next();
		return entity.isEventBatch() ? toStoredTestEventBatch(entities, bookId) : toStoredTestEventSingle(entities, bookId);
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
}

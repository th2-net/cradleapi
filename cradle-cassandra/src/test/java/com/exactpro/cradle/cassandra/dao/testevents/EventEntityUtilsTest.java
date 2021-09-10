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
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.zip.DataFormatException;

import org.assertj.core.api.Assertions;
import org.assertj.core.api.recursive.comparison.RecursiveComparisonConfiguration;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.exactpro.cradle.BookId;
import com.exactpro.cradle.Direction;
import com.exactpro.cradle.PageId;
import com.exactpro.cradle.messages.StoredMessageId;
import com.exactpro.cradle.testevents.StoredTestEvent;
import com.exactpro.cradle.testevents.StoredTestEventId;
import com.exactpro.cradle.testevents.TestEventBatchToStore;
import com.exactpro.cradle.testevents.TestEventSingleToStore;
import com.exactpro.cradle.testevents.TestEventSingleToStoreBuilder;
import com.exactpro.cradle.testevents.TestEventToStore;
import com.exactpro.cradle.utils.CradleIdException;
import com.exactpro.cradle.utils.CradleStorageException;

import static com.exactpro.cradle.cassandra.TestUtils.createContent;

public class EventEntityUtilsTest
{
	private final BookId book = new BookId("Book1");
	private final PageId page = new PageId(book, "Page1");
	private final String scope = "Scope1";
	private final Instant startTimestamp = Instant.now();
	private final StoredTestEventId eventId = new StoredTestEventId(book, scope, startTimestamp, "EventId"),
			parentId = new StoredTestEventId(book, scope, startTimestamp, "ParentEventId");
	
	private TestEventSingleToStoreBuilder singleBuilder = TestEventSingleToStore.builder();
	private final int contentChunk = 20,
			content0_5 = 10,
			content1_5 = 35,
			content2 = 40,
			content3 = 60,
			messagesChunk = 10,
			messages0_5 = 5,
			messages1_5 = 15,
			messages2 = 20;
	
	@DataProvider(name = "events")
	public Object[][] events() throws CradleStorageException
	{
		TestEventBatchToStore batch = TestEventBatchToStore.builder()
				.id(new StoredTestEventId(book, scope, startTimestamp, "BatchId"))
				.parentId(parentId)
				.build();
		batch.addTestEvent(prepareSingle().content(createContent(content0_5)).build());
		return new Object[][]
				{
					{prepareSingle().content(createContent(content0_5)).build()},
					{prepareSingle().content(createContent(content1_5)).build()},
					{prepareSingle().content(createContent(content2)).build()},
					{prepareSingle().messages(createMessageIds(messages0_5)).build()},
					{prepareSingle().messages(createMessageIds(messages1_5)).build()},
					{prepareSingle().messages(createMessageIds(messages2)).build()},
					{prepareSingle().content(createContent(content0_5)).messages(createMessageIds(messages0_5)).build()},
					{prepareSingle().content(createContent(content2)).messages(createMessageIds(messages2)).build()},
					{prepareSingle().content(createContent(content0_5)).messages(createMessageIds(messages1_5)).build()},
					{prepareSingle().content(createContent(content1_5)).messages(createMessageIds(messages0_5)).build()},
					{batch}
				};
	}
	
	@DataProvider(name = "corrupted events")
	public Object[][] corruptedEvents() throws CradleStorageException
	{
		TestEventToStore event = prepareSingle().content(createContent(content3)).build();
		return new Object[][]
				{
					{event, 2, "Last chunk"},
					{event, 1, "Chunk #1"}
				};
	}
	
	
	private TestEventSingleToStoreBuilder prepareSingle()
	{
		return singleBuilder
				.id(eventId)
				.parentId(parentId)
				.name("TestEvent1")
				.type("Type1");
	}
	
	private Set<StoredMessageId> createMessageIds(int size)
	{
		Set<StoredMessageId> result = new HashSet<>();
		for (int i = 0; i < size; i++)
			result.add(new StoredMessageId(book, "Session1", Direction.FIRST, startTimestamp, i));
		return result;
	}
	
	
	@Test(dataProvider = "events")
	public void eventEntity(TestEventToStore event) throws CradleStorageException, IOException, DataFormatException, CradleIdException
	{
		List<TestEventEntity> entities = EventEntityUtils.toEntities(event, page, 2000, contentChunk, messagesChunk);
		StoredTestEvent newEvent = EventEntityUtils.toStoredTestEvent(entities, page);
		
		RecursiveComparisonConfiguration config = new RecursiveComparisonConfiguration();
		config.ignoreFieldsMatchingRegexes("pageId", ".*\\.pageId", "error", ".*\\.error");
		
		Assertions.assertThat(newEvent)
				.usingRecursiveComparison(config)
				.isEqualTo(event);
	}
	
	@Test(dataProvider = "corrupted events")
	public void corruptedEvent(TestEventToStore event, int chunkToRemove, String expectedError) throws CradleStorageException, IOException, DataFormatException, CradleIdException
	{
		List<TestEventEntity> entities = EventEntityUtils.toEntities(event, page, 2000, contentChunk, messagesChunk);
		entities.remove(chunkToRemove);
		StoredTestEvent newEvent = EventEntityUtils.toStoredTestEvent(entities, page);
		
		Assert.assertTrue(newEvent.getError() != null && newEvent.getError().contains(expectedError), 
				"corrupted event has '"+expectedError+"' in error");
	}
}
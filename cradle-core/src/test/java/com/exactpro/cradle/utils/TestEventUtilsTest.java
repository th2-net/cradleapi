/*
 * Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.cradle.utils;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.exactpro.cradle.Direction;
import com.exactpro.cradle.messages.StoredMessageId;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.exactpro.cradle.testevents.StoredTestEventBatch;
import com.exactpro.cradle.testevents.StoredTestEventId;
import com.exactpro.cradle.testevents.TestEventBatchToStore;
import com.exactpro.cradle.testevents.TestEventToStore;
import com.exactpro.cradle.testevents.TestEventToStoreBuilder;

public class TestEventUtilsTest
{
	private static final StoredTestEventId DUMMY_ID = new StoredTestEventId("123");
	private static final String DUMMY_NAME = "TestEvent";
	private static final String DUMMY_STREAM_NAME = "TestStream";
	private static final Instant DUMMY_START_TIMESTAMP = Instant.now();
	private static final String DUMMY_X_ALIAS = "dummyX_alias954",
			TEST_ALIAS = "test_alias",
			ANOTHER_ALIAS = "another_alias";

	private TestEventToStoreBuilder eventBuilder;

	@BeforeClass
	public void prepare() throws CradleStorageException
	{
		eventBuilder = new TestEventToStoreBuilder();
	}
	
	@DataProvider(name = "invalid events")
	public Object[][] invalidEvents()
	{
		return new Object[][]
				{
					{eventBuilder.build()},  //Empty event
					{eventBuilder.id(DUMMY_ID).build()},
					{eventBuilder.name(DUMMY_NAME).build()},
					{eventBuilder.startTimestamp(DUMMY_START_TIMESTAMP).build()},
					{eventBuilder.id(DUMMY_ID).name(DUMMY_NAME).build()},
					{eventBuilder.id(DUMMY_ID).startTimestamp(DUMMY_START_TIMESTAMP).build()},
					{eventBuilder.name(DUMMY_NAME).startTimestamp(DUMMY_START_TIMESTAMP).build()}
				};
	}
	
	@DataProvider(name = "linkedIds")
	public Object[][] linkedIds()
	{
		return new Object[][]
				{
					{null},
					{Collections.singleton(new StoredMessageId("aliasXYZ", Direction.FIRST, 1631071200662515000L))},
					{
						Arrays.asList(new StoredMessageId(DUMMY_X_ALIAS, Direction.FIRST, 1631071200662515748L),
								new StoredMessageId(DUMMY_X_ALIAS, Direction.FIRST, 1631071200662515749L),
								new StoredMessageId(TEST_ALIAS, Direction.FIRST, 1631071200662515750L),
								new StoredMessageId(DUMMY_X_ALIAS, Direction.FIRST, 1631071200662515750L),
								new StoredMessageId(TEST_ALIAS, Direction.SECOND, 1631071200662515750L),
								new StoredMessageId(TEST_ALIAS, Direction.FIRST, 1631071200662515749L),
								new StoredMessageId(DUMMY_X_ALIAS, Direction.FIRST, 1631071200662515751L),
								new StoredMessageId(TEST_ALIAS, Direction.FIRST, 1631071200662515752L),
								new StoredMessageId(DUMMY_X_ALIAS, Direction.FIRST, 1631071200662515752L))
					}
				};
	}
	
	@DataProvider(name = "batchLinkedIds")
	public Object[][] batchLinkedIds()
	{
		Map<StoredTestEventId, Collection<StoredMessageId>> batch = new HashMap<>();
		batch.put(new StoredTestEventId("regularEvent"), 
				Arrays.asList(new StoredMessageId(DUMMY_X_ALIAS, Direction.FIRST, 1631071200662515748L),
						new StoredMessageId(DUMMY_X_ALIAS, Direction.FIRST, 1631071200662515747L),
						new StoredMessageId(TEST_ALIAS, Direction.FIRST, 1631071200662515749L)));
		batch.put(new StoredTestEventId("emptyEvent"), new ArrayList<>());
		batch.put(new StoredTestEventId("anotherEvent"), 
				Arrays.asList(new StoredMessageId(DUMMY_X_ALIAS, Direction.FIRST, 1631071200662515748L),
						new StoredMessageId(TEST_ALIAS, Direction.FIRST, 1631071200662515700L),
						new StoredMessageId(TEST_ALIAS, Direction.FIRST, 1631071200662515701L),
						new StoredMessageId(ANOTHER_ALIAS, Direction.FIRST, 2223L)));
		
		return new Object[][]
				{
					{null},
					{batch}
				};
	}
	
	@Test(dataProvider = "invalid events",
			expectedExceptions = CradleStorageException.class)
	public void eventValidation(TestEventToStore event) throws CradleStorageException
	{
		TestEventUtils.validateTestEvent(event, true);
	}
	
	@Test
	public void validEvent() throws CradleStorageException
	{
		TestEventToStore event = eventBuilder.id(DUMMY_ID)
				.name(DUMMY_NAME)
				.startTimestamp(DUMMY_START_TIMESTAMP)
				.content("Test content".getBytes())
				.build();
		TestEventUtils.validateTestEvent(event, true);
	}
	
	@Test
	public void validBatchEvent() throws CradleStorageException
	{
		StoredTestEventBatch batch = generateBatch();
		TestEventUtils.validateTestEvent(batch, false);
	}
	
	@Test(dataProvider = "linkedIds")
	public void linkedIds(Collection<StoredMessageId> links) throws IOException
	{
		byte[] bytes = TestEventUtils.serializeLinkedMessageIds(links);
		Collection<StoredMessageId> restored = TestEventUtils.deserializeLinkedMessageIds(bytes);
		
		if (links == null)
		{
			Assert.assertNull(restored, "deserialized IDs are null");
			return;
		}
		
		Assert.assertEquals(restored.size(), links.size(), "size of deserialized IDs collection");
		
		restored.removeAll(links);
		Assert.assertEquals(restored.size(), 0, "number of extra elements");
	}
	
	@Test(dataProvider = "batchLinkedIds")
	public void batchLinkedIds(Map<StoredTestEventId, Collection<StoredMessageId>> batchLinks) throws IOException
	{
		byte[] bytes = TestEventUtils.serializeBatchLinkedMessageIds(batchLinks);
		Map<StoredTestEventId, Collection<StoredMessageId>> restored = TestEventUtils.deserializeBatchLinkedMessageIds(bytes);
		
		if (batchLinks == null)
		{
			Assert.assertNull(restored, "deserialized IDs are null");
			return;
		}
		
		Assert.assertEquals(restored.size(), batchLinks.size(), "size of deserialized map");
		for (Entry<StoredTestEventId, Collection<StoredMessageId>> eventLinks : batchLinks.entrySet())
		{
			StoredTestEventId eventId = eventLinks.getKey();
			Collection<StoredMessageId> links = eventLinks.getValue();
			Collection<StoredMessageId> restoredLinks = restored.remove(eventId);
			Assert.assertEquals(restoredLinks.size(), links.size(), "size of deserialized IDs collection for event "+eventId);
			
			restoredLinks.removeAll(links);
			Assert.assertEquals(restoredLinks.size(), 0, "number of extra elements for event "+eventId);
		}
		
		Assert.assertEquals(restored.size(), 0, "number of extra elements in map");
	}
	

	private StoredTestEventBatch generateBatch() throws CradleStorageException
	{
		StoredTestEventId parentId = new StoredTestEventId("ParentID");
		TestEventToStore event = eventBuilder.id(DUMMY_ID)
				.name(DUMMY_NAME)
				.startTimestamp(DUMMY_START_TIMESTAMP)
				.parentId(parentId)
				.messageIds(generateMessageIds())
				.content("Test content".getBytes())
				.build();

		TestEventBatchToStore batchToStore = new TestEventBatchToStore();
		batchToStore.setParentId(parentId);

		StoredTestEventBatch batch = new StoredTestEventBatch(batchToStore);
		batch.addTestEvent(event);

		return batch;
	}

	private Collection<StoredMessageId> generateMessageIds()
	{
		Collection<StoredMessageId> result = new ArrayList<>();
		for (int i = 0; i < 10; i++)
		{
			result.add(new StoredMessageId(DUMMY_STREAM_NAME, Direction.FIRST, i));
		}
		return result;
	}

}

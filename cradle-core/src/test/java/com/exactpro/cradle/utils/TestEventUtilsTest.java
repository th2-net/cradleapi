/*
 * Copyright 2020-2021 Exactpro (Exactpro Systems Limited)
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

import java.time.Instant;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.exactpro.cradle.BookId;
import com.exactpro.cradle.testevents.StoredTestEventBatch;
import com.exactpro.cradle.testevents.StoredTestEventId;
import com.exactpro.cradle.testevents.TestEventBatchToStore;
import com.exactpro.cradle.testevents.TestEventSingleToStore;
import com.exactpro.cradle.testevents.TestEventSingleToStoreBuilder;

public class TestEventUtilsTest
{
	private static final BookId DUMMY_BOOK = new BookId("book1");
	private static final StoredTestEventId DUMMY_ID = new StoredTestEventId(DUMMY_BOOK, Instant.EPOCH, "123"),
			BROKEN_ID = new StoredTestEventId(null, null, "123");
	private static final String DUMMY_NAME = "TestEvent";
	
	private TestEventSingleToStoreBuilder eventBuilder;
	
	@BeforeClass
	public void prepare() throws CradleStorageException
	{
		eventBuilder = new TestEventSingleToStoreBuilder();
	}
	
	@DataProvider(name = "invalid events")
	public Object[][] invalidEvents()
	{
		return new Object[][]
				{
					{eventBuilder.build()},  //Empty event
					{eventBuilder.id(DUMMY_ID).build()},
					{eventBuilder.name(DUMMY_NAME).build()},
					{eventBuilder.id(BROKEN_ID).name(DUMMY_NAME).build()}
				};
	}
	
	@Test(dataProvider = "invalid events",
			expectedExceptions = CradleStorageException.class)
	public void eventValidation(TestEventSingleToStore event) throws CradleStorageException
	{
		TestEventUtils.validateTestEvent(event, true);
	}
	
	@Test
	public void validEvent() throws CradleStorageException
	{
		TestEventSingleToStore event = eventBuilder.id(DUMMY_ID)
				.name(DUMMY_NAME)
				.content("Test content".getBytes())
				.build();
		TestEventUtils.validateTestEvent(event, true);
	}
	
	@Test
	public void validBatchEvent() throws CradleStorageException
	{
		StoredTestEventId batchId = new StoredTestEventId(DUMMY_BOOK, Instant.EPOCH, "BatchID"),
				parentId = new StoredTestEventId(DUMMY_BOOK, Instant.EPOCH, "ParentID");
		TestEventSingleToStore event = eventBuilder.id(DUMMY_ID)
				.name(DUMMY_NAME)
				.parentId(parentId)
				.content("Test content".getBytes())
				.build();
		
		TestEventBatchToStore batchToStore = new TestEventBatchToStore();
		batchToStore.setId(batchId);
		batchToStore.setParentId(parentId);
		
		StoredTestEventBatch batch = new StoredTestEventBatch(batchToStore);
		batch.addTestEvent(event);
		TestEventUtils.validateTestEvent(batch, false);
	}
}

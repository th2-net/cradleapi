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

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.exactpro.cradle.books.BookId;
import com.exactpro.cradle.testevents.StoredTestEventBatch;
import com.exactpro.cradle.testevents.StoredTestEventId;
import com.exactpro.cradle.testevents.TestEventBatchToStore;
import com.exactpro.cradle.testevents.TestEventSingleToStore;
import com.exactpro.cradle.testevents.TestEventSingleToStoreBuilder;

public class TestEventUtilsTest
{
	private static final BookId BOOK = new BookId("book1");
	private static final String SCOPE = "default",
			ID_VALUE = "123";
	private static final Instant START_TIMESTAMP = Instant.EPOCH;
	private static final StoredTestEventId DUMMY_ID = new StoredTestEventId(BOOK, SCOPE, START_TIMESTAMP, ID_VALUE);
	private static final String DUMMY_NAME = "TestEvent";
	
	@DataProvider(name = "invalid events")
	public Object[][] invalidEvents()
	{
		return new Object[][]
				{
					{new TestEventSingleToStoreBuilder().build(),                                        //Empty event
							"ID cannot be null"},
					{event().name(null).build(),                                                         //No name
							"must have a name"},
        	{event().id(null).build(),                                                           //No ID
        			"ID cannot be null"},
        	{event().id(new StoredTestEventId(null, SCOPE, START_TIMESTAMP, ID_VALUE)).build(),  //No book
        			"must have a book"},
        	{event().id(new StoredTestEventId(BOOK, null, START_TIMESTAMP, ID_VALUE)).build(),   //No scope
        			"must have a scope"},
        	{event().id(new StoredTestEventId(BOOK, SCOPE, null, ID_VALUE)).build(),             //No timestamp
        			"must have a start timestamp"}
				};
	}
	
	private TestEventSingleToStoreBuilder event()
	{
		//Preparing valid event. It will be made invalid in "invalid events"
		return new TestEventSingleToStoreBuilder()
				.id(DUMMY_ID)
				.name(DUMMY_NAME);
	}
	
	
	@Test(dataProvider = "invalid events",
			expectedExceptions = CradleStorageException.class)
	public void eventValidation(TestEventSingleToStore event, String errorMessage) throws CradleStorageException
	{
		try
		{
			TestEventUtils.validateTestEvent(event, true);
		}
		catch (CradleStorageException e)
		{
			Assert.assertTrue(e.getMessage().contains(errorMessage), "error contains '"+errorMessage+"'");
			throw e;
		}
	}
	
	@Test
	public void validEvent() throws CradleStorageException
	{
		TestEventSingleToStore event = new TestEventSingleToStoreBuilder().id(DUMMY_ID)
				.name(DUMMY_NAME)
				.content("Test content".getBytes())
				.build();
		TestEventUtils.validateTestEvent(event, true);
	}
	
	@Test
	public void validBatchEvent() throws CradleStorageException
	{
		StoredTestEventId batchId = new StoredTestEventId(BOOK, SCOPE, Instant.EPOCH, "BatchID"),
				parentId = new StoredTestEventId(BOOK, SCOPE, Instant.EPOCH, "ParentID");
		TestEventSingleToStore event = new TestEventSingleToStoreBuilder().id(DUMMY_ID)
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

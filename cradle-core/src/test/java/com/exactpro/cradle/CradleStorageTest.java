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

package com.exactpro.cradle;

import java.io.IOException;
import java.time.Instant;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.exactpro.cradle.testevents.StoredTestEventId;
import com.exactpro.cradle.testevents.TestEventSingleToStoreBuilder;
import com.exactpro.cradle.utils.CradleStorageException;

public class CradleStorageTest
{
	private final String BOOK = "Book1",
			PAGE = "Page1",
			SCOPE = "Scope1",
			EVENT_ID = "Event1";
	private final BookId BOOK_ID = new BookId(BOOK);
	private final Instant START_TIMESTAMP = Instant.now().plusSeconds(10000);
	private final StoredTestEventId DUMMY_EVENT_ID = new StoredTestEventId(BOOK_ID, SCOPE, START_TIMESTAMP, EVENT_ID);
	
	private CradleStorage storage;
	
	@BeforeMethod
	public void prepare() throws CradleStorageException
	{
		storage = new DummyCradleStorage();
		storage.init(false);
		storage.addBook(BOOK, null, null, PAGE, null);
	}
	
	@DataProvider(name = "invalid events")
	public Object[][] invalidEvents() throws CradleStorageException
	{
		return new Object[][]
				{
					{validEvent()
							.id(new BookId(BOOK+"_X"), SCOPE, START_TIMESTAMP, EVENT_ID),          //Unknown book
							"unknown"},
					{validEvent()
							.id(new BookId(BOOK), SCOPE, Instant.EPOCH, EVENT_ID),                 //Too early
							"started after"},
					{validEvent().endTimestamp(START_TIMESTAMP.minusMillis(5000)),                 //End before start
							"cannot end sooner than it started"}
				};
	}
	
	public TestEventSingleToStoreBuilder validEvent()
	{
		//Preparing valid event. It will be made invalid in "invalid events"
		return new TestEventSingleToStoreBuilder()
				.id(DUMMY_EVENT_ID)
				.name("Event1");
	}
	
	@Test(dataProvider = "invalid events", expectedExceptions = {CradleStorageException.class})
	public void storeEventUnknownBook(TestEventSingleToStoreBuilder builder, String errorMessage) throws IOException, CradleStorageException
	{
		try
		{
			storage.storeTestEvent(builder.build());
		}
		catch (CradleStorageException e)
		{
			TestUtils.handleException(e, errorMessage);
		}
	}
}

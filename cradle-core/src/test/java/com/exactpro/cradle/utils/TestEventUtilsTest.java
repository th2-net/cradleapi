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

import java.time.Instant;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.exactpro.cradle.testevents.StoredTestEventId;
import com.exactpro.cradle.testevents.TestEventToStore;
import com.exactpro.cradle.testevents.TestEventToStoreBuilder;
import com.exactpro.cradle.utils.CradleStorageException;

public class TestEventUtilsTest
{
	private static final StoredTestEventId DUMMY_ID = new StoredTestEventId("123");
	private static final String DUMMY_NAME = "TestEvent";
	private static final Instant DUMMY_START_TIMESTAMP = Instant.now();
	
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
	
	@Test(dataProvider = "invalid events",
			expectedExceptions = CradleStorageException.class)
	public void eventValidationl(TestEventToStore event) throws CradleStorageException
	{
		TestEventUtils.validateTestEvent(event);
	}
	
	@Test
	public void validEvent() throws CradleStorageException
	{
		TestEventToStore event = eventBuilder.id(new StoredTestEventId("123"))
				.name("TestEvent")
				.startTimestamp(Instant.now())
				.content("Test content".getBytes())
				.build();
		TestEventUtils.validateTestEvent(event);
	}
}

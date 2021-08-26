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

package com.exactpro.cradle.testevents;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;

import org.assertj.core.api.Assertions;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.exactpro.cradle.BookId;
import com.exactpro.cradle.Direction;
import com.exactpro.cradle.messages.StoredMessageId;
import com.exactpro.cradle.utils.CradleIdException;
import com.exactpro.cradle.utils.CradleStorageException;
import com.exactpro.cradle.utils.TestEventUtils;

public class TestEventTest
{
	private final BookId BOOK = new BookId("book1");
	private final String SCOPE = "default",
			DUMMY_NAME = "TestEvent",
			ID_VALUE = "Event_ID";
	private final Instant START_TIMESTAMP = Instant.now();
	private final StoredTestEventId DUMMY_ID = new StoredTestEventId(BOOK, SCOPE, START_TIMESTAMP, ID_VALUE);
	
	private TestEventSingleToStoreBuilder eventBuilder = TestEventToStore.singleBuilder();
	private TestEventBatchToStoreBuilder batchBuilder = TestEventToStore.batchBuilder();
	private StoredTestEventId batchId = new StoredTestEventId(BOOK, SCOPE, START_TIMESTAMP, UUID.randomUUID().toString()),
			batchParentId = new StoredTestEventId(BOOK, SCOPE, START_TIMESTAMP, "BatchParentID");
	private TestEventBatchToStore batch;
	
	@BeforeMethod
	public void prepareBatch() throws CradleStorageException
	{
		batch = batchBuilder
				.id(batchId)
				.parentId(batchParentId)
				.build();
	}
	
	@DataProvider(name = "invalid events")
	public Object[][] invalidEvents() throws CradleStorageException
	{
		return new Object[][]
				{
					{new TestEventSingleToStoreBuilder(),                                                             //Empty event
							"ID cannot be null"},
					{validEvent().id(null),                                                                           //No ID
							"ID cannot be null"},
					{validEvent().id(new StoredTestEventId(null, SCOPE, START_TIMESTAMP, ID_VALUE)),                  //No book
							"must have a book"},
					{validEvent().id(new StoredTestEventId(BOOK, null, START_TIMESTAMP, ID_VALUE)),                   //No scope
							"must have a scope"},
					{validEvent().id(new StoredTestEventId(BOOK, SCOPE, null, ID_VALUE)),                             //No timestamp
							"must have a start timestamp"},
					{validEvent().name(null),                                                                         //No name
							"must have a name"},
					{validEvent().parentId(DUMMY_ID),                                                                 //Self-reference
							"reference itself"},
					{validEvent().endTimestamp(START_TIMESTAMP.minusMillis(5000)),                                    //End before start
							"cannot end sooner than it started"},
					{validEvent().messages(Collections.singleton(new StoredMessageId(new BookId(BOOK.getName()+"1"),  //Different book in message
							"Session1", Direction.FIRST, START_TIMESTAMP, 1))), "Book of message"}
				};
	}
	
	@DataProvider(name = "batch invalid events")
	public Object[][] batchInvalidEvents() throws CradleStorageException
	{
		Object[][] batchEvents = new Object[][]
				{
					{validEvent().parentId(null),                                                                      //No parent ID
								"must have a parent"},
					{validEvent().id(new StoredTestEventId(new BookId(BOOK.getName()+"1"),                             //Different book
									SCOPE, START_TIMESTAMP, DUMMY_NAME))
							.parentId(new StoredTestEventId(new BookId(BOOK.getName()+"1"), 
									SCOPE, START_TIMESTAMP, "Parent_"+DUMMY_NAME)), 
							"events of book"},
					{validEvent().id(new StoredTestEventId(BOOK, SCOPE+"1", START_TIMESTAMP, DUMMY_NAME)),             //Different scope
							"events of scope"},
					{validEvent().id(new StoredTestEventId(BOOK, SCOPE,                                                //Early timestamp
							START_TIMESTAMP.minusMillis(5000), DUMMY_NAME)),
							"start timestamp"},
					{validEvent().parentId(new StoredTestEventId(BOOK, SCOPE, START_TIMESTAMP, "wrong_parent")),       //Different parent ID
							"parent"},
					{validEvent().parentId(null),                                                                      //No parent
							"must have a parent"}
				};
		
		return Stream.of(invalidEvents(), batchEvents)
				.flatMap(Arrays::stream)
				.toArray(Object[][]::new);
	}
	
	
	private TestEventSingleToStoreBuilder validEvent()
	{
		//Preparing valid event that corresponds to the batch. It will be made invalid in "invalid events"
		return new TestEventSingleToStoreBuilder()
				.id(DUMMY_ID)
				.parentId(batchParentId)
				.name(DUMMY_NAME);
	}
	
	
	@Test
	public void eventFields() throws CradleStorageException
	{
		Set<StoredMessageId> messages = new HashSet<>();
		messages.add(new StoredMessageId(BOOK, "Session1", Direction.FIRST, Instant.EPOCH, 1));
		messages.add(new StoredMessageId(BOOK, "Session2", Direction.SECOND, Instant.EPOCH, 2));
		
		TestEventSingleToStore event = eventBuilder
				.id(DUMMY_ID)
				.name("Name1")
				.parentId(batchParentId)
				.type("Type1")
				.success(true)
				.endTimestamp(Instant.now())
				.messages(messages)
				.content("Valid content".getBytes())
				.build();
		StoredTestEventSingle stored = new StoredTestEventSingle(event);
		
		Assertions.assertThat(stored)
				.usingRecursiveComparison()
				.isEqualTo(event);
	}
	
	@Test
	public void batchFields() throws CradleStorageException
	{
		TestEventBatchToStore event = TestEventToStore.batchBuilder()
				.id(DUMMY_ID)
				.name("Name1")
				.parentId(batchParentId)
				.type("Type1")
				.build();
		
		Set<StoredMessageId> messages1 = Collections.singleton(new StoredMessageId(BOOK, "Session1", Direction.FIRST, Instant.EPOCH, 1));
		event.addTestEvent(eventBuilder.id(new StoredTestEventId(BOOK, SCOPE, START_TIMESTAMP.plusMillis(3500), ID_VALUE))
				.name(DUMMY_NAME)
				.parentId(batch.getParentId())
				.endTimestamp(START_TIMESTAMP.plusMillis(5000))
				.messages(messages1)
				.success(true)
				.build());
		
		Set<StoredMessageId> messages2 = Collections.singleton(new StoredMessageId(BOOK, "Session2", Direction.SECOND, Instant.EPOCH, 2));
		event.addTestEvent(eventBuilder.id(new StoredTestEventId(BOOK, SCOPE, START_TIMESTAMP, ID_VALUE+"1"))
				.name(DUMMY_NAME)
				.parentId(batch.getParentId())
				.messages(messages2)
				.success(false)
				.build());
		
		event.addTestEvent(eventBuilder.id(new StoredTestEventId(BOOK, SCOPE, START_TIMESTAMP, ID_VALUE+"2"))
				.name(DUMMY_NAME)
				.parentId(batch.getParentId())
				.success(false)
				.build());
		
		StoredTestEventBatch stored = new StoredTestEventBatch(event);
		
		Assertions.assertThat(stored)
				.usingRecursiveComparison()
				.isEqualTo(event);
	}
	
	@Test(dataProvider = "invalid events",
			expectedExceptions = {CradleStorageException.class})
	public void eventValidation(TestEventSingleToStoreBuilder builder, String errorMessage) throws CradleStorageException
	{
		try
		{
			TestEventUtils.validateTestEvent(builder.build());
		}
		catch (CradleStorageException e)
		{
			Assert.assertTrue(e.getMessage().contains(errorMessage), "error contains '"+errorMessage+"'");
			throw e;
		}
	}
	
	@Test
	public void passedEvent() throws CradleStorageException
	{
		TestEventSingleToStore event = eventBuilder
				.id(DUMMY_ID)
				.name(DUMMY_NAME)
				.content("Test content".getBytes())
				.build();
		TestEventUtils.validateTestEvent(event);
	}
	
	@Test
	public void passedBatchEvent() throws CradleStorageException
	{
		StoredTestEventId batchId = new StoredTestEventId(BOOK, SCOPE, Instant.EPOCH, "BatchID"),
				parentId = new StoredTestEventId(BOOK, SCOPE, Instant.EPOCH, "ParentID");
		TestEventSingleToStore event = eventBuilder
				.id(DUMMY_ID)
				.name(DUMMY_NAME)
				.parentId(parentId)
				.content("Test content".getBytes())
				.build();
		
		TestEventBatchToStore batch = new TestEventBatchToStore(batchId, null, parentId);
		batch.addTestEvent(event);
		TestEventUtils.validateTestEvent(batch);
	}
	
	@Test(expectedExceptions = {CradleStorageException.class}, expectedExceptionsMessageRegExp = "Batch must have a parent")
	public void batchParentMustBeSet() throws CradleStorageException
	{
		batchBuilder.id(DUMMY_ID).build();
	}
	
	@Test(expectedExceptions = {CradleStorageException.class}, expectedExceptionsMessageRegExp = "Test event with ID .* is already present in batch")
	public void duplicateIds() throws CradleIdException, CradleStorageException
	{
		StoredTestEventId eventId = new StoredTestEventId(BOOK, SCOPE, START_TIMESTAMP, "AAA");
		batch.addTestEvent(eventBuilder.id(eventId).name(DUMMY_NAME).parentId(batch.getParentId()).build());
		batch.addTestEvent(eventBuilder.id(eventId).name(DUMMY_NAME).parentId(batch.getParentId()).build());
	}
	
	@Test(expectedExceptions = {CradleStorageException.class}, expectedExceptionsMessageRegExp = ".* '.*\\:XXX' .* stored in this batch .*")
	public void externalReferences() throws CradleIdException, CradleStorageException
	{
		StoredTestEventId parentId = new StoredTestEventId(BOOK, SCOPE, START_TIMESTAMP, "1");
		batch.addTestEvent(eventBuilder.id(parentId)
				.name(DUMMY_NAME)
				.parentId(batch.getParentId())
				.build());
		batch.addTestEvent(eventBuilder.id(new StoredTestEventId(BOOK, SCOPE, START_TIMESTAMP, "2"))
				.name(DUMMY_NAME)
				.parentId(batch.getParentId())
				.build());
		batch.addTestEvent(eventBuilder.id(new StoredTestEventId(BOOK, SCOPE, START_TIMESTAMP, "3"))
				.name(DUMMY_NAME)
				.parentId(parentId)
				.build());
		batch.addTestEvent(eventBuilder.id(new StoredTestEventId(BOOK, SCOPE, START_TIMESTAMP, "4"))
				.name(DUMMY_NAME)
				.parentId(new StoredTestEventId(BOOK, SCOPE, START_TIMESTAMP, "XXX"))
				.build());
	}
	
	@Test(expectedExceptions = {CradleStorageException.class}, expectedExceptionsMessageRegExp = ".* stored in this batch .*")
	public void referenceToBatch() throws CradleIdException, CradleStorageException
	{
		StoredTestEventId parentId = new StoredTestEventId(BOOK, SCOPE, START_TIMESTAMP, "1");
		batch.addTestEvent(eventBuilder.id(parentId)
				.name(DUMMY_NAME)
				.parentId(batch.getParentId())
				.build());
		batch.addTestEvent(eventBuilder.id(new StoredTestEventId(BOOK, SCOPE, START_TIMESTAMP, "2"))
				.name(DUMMY_NAME)
				.parentId(batch.getId())
				.build());
	}
	
	@Test
	public void childrenAligned() throws CradleStorageException
	{
		BatchedStoredTestEvent parentEvent = batch.addTestEvent(eventBuilder
						.id(new StoredTestEventId(BOOK, SCOPE, START_TIMESTAMP, "1"))
						.name(DUMMY_NAME)
						.parentId(batch.getParentId())
						.build()),
				childEvent = batch.addTestEvent(eventBuilder
						.id(new StoredTestEventId(BOOK, SCOPE, START_TIMESTAMP, "2"))
						.name(DUMMY_NAME)
						.parentId(parentEvent.getId())
						.build());
		
		Assert.assertEquals(parentEvent.getChildren().contains(childEvent), true, "Children are aligned with their parent");
	}
	
	@Test
	public void rootIsRoot() throws CradleStorageException
	{
		BatchedStoredTestEvent parentEvent = batch.addTestEvent(eventBuilder
				.id(DUMMY_ID)
				.name(DUMMY_NAME)
				.parentId(batch.getParentId())
				.build());
		Assert.assertEquals(batch.getRootTestEvents().contains(parentEvent), true, "Root event is listed in roots");
	}
	
	@Test
	public void childIsNotRoot() throws CradleStorageException
	{
		BatchedStoredTestEvent parentEvent = batch.addTestEvent(eventBuilder
						.id(new StoredTestEventId(BOOK, SCOPE, START_TIMESTAMP, "1"))
						.name(DUMMY_NAME)
						.parentId(batch.getParentId())
						.build()),
				childEvent = batch.addTestEvent(eventBuilder
						.id(new StoredTestEventId(BOOK, SCOPE, START_TIMESTAMP, "2"))
						.name(DUMMY_NAME)
						.parentId(parentEvent.getId())
						.build());

		Assert.assertEquals(batch.getRootTestEvents().contains(childEvent), false, "Child event is not listed in roots");
	}
	
	@Test(dataProvider = "batch invalid events",
			expectedExceptions = {CradleStorageException.class})
	public void batchEventValidation(TestEventSingleToStoreBuilder builder, String errorMessage) throws CradleStorageException
	{
		try
		{
			batch.addTestEvent(builder.build());
		}
		catch (CradleStorageException e)
		{
			Assert.assertTrue(e.getMessage().contains(errorMessage), "error contains '"+errorMessage+"'");
			throw e;
		}
	}
}

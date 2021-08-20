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
import java.util.UUID;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.exactpro.cradle.books.BookId;
import com.exactpro.cradle.utils.CradleIdException;
import com.exactpro.cradle.utils.CradleStorageException;

public class StoredTestEventTest
{
	private static final BookId BOOK = new BookId("book1");
	private static final String SCOPE = "default",
			DUMMY_NAME = "TestEvent",
			ID_VALUE = "Event_ID";
	private static final Instant START_TIMESTAMP = Instant.now();
	private static final StoredTestEventId DUMMY_ID = new StoredTestEventId(BOOK, SCOPE, START_TIMESTAMP, ID_VALUE);
	
	private TestEventBatchToStoreBuilder batchSettingsBuilder;
	private TestEventSingleToStoreBuilder eventBuilder;
	private StoredTestEventId batchId,
			batchParentId;
	private TestEventBatchToStore batchSettings;
	private StoredTestEventBatch batch;
	
	@BeforeClass
	public void prepare()
	{
		batchSettingsBuilder = TestEventBatchToStore.builder();
		eventBuilder = new TestEventSingleToStoreBuilder();
		batchId = new StoredTestEventId(BOOK, SCOPE, START_TIMESTAMP, UUID.randomUUID().toString());
		batchParentId = new StoredTestEventId(BOOK, SCOPE, START_TIMESTAMP, "BatchParentID");
		batchSettings = batchSettingsBuilder
				.id(batchId)
				.parentId(batchParentId)
				.build();
	}
	
	@BeforeMethod
	public void prepareBatch() throws CradleStorageException
	{
		batch = new StoredTestEventBatch(batchSettings);
	}
	
	@DataProvider(name = "invalid events")
	public Object[][] invalidEvents()
	{
		return new Object[][]
				{
					{new TestEventSingleToStoreBuilder().build(),                                                    //Empty event
							"ID cannot be null"},
					{validEvent().id(null).build(),                                                                  //No ID
							"ID cannot be null"},
					{validEvent().id(new StoredTestEventId(null, SCOPE, START_TIMESTAMP, ID_VALUE)).build(),         //No book
							"must have a book"},
					{validEvent().id(new StoredTestEventId(BOOK, null, START_TIMESTAMP, ID_VALUE)).build(),          //No scope
							"must have a scope"},
					{validEvent().id(new StoredTestEventId(BOOK, SCOPE, null, ID_VALUE)).build(),                    //No timestamp
							"must have a start timestamp"},
					{validEvent().name(null).build(),                                                                //No name
							"must have a name"},
					{validEvent().parentId(null).build(),                                                            //No parent ID
								"must have a parent"},
					{validEvent().id(new StoredTestEventId(new BookId(BOOK.getName()+"1"),                           //Different book
							SCOPE, START_TIMESTAMP, DUMMY_NAME)).build(), 
							"events of book"},
					{validEvent().id(new StoredTestEventId(BOOK, SCOPE+"1", START_TIMESTAMP, DUMMY_NAME)).build(),  //Different scope
							"events of scope"},
					{validEvent().id(new StoredTestEventId(BOOK, SCOPE,                                             //Early timestamp
							START_TIMESTAMP.minusMillis(5000), DUMMY_NAME)).build(),
							"start timestamp"},
					{validEvent().parentId(new StoredTestEventId(BOOK, SCOPE, START_TIMESTAMP, "wrong_parent"))     //Different parent ID
								.build(), "parent"}
				};
	}
	
	
	private TestEventSingleToStoreBuilder validEvent()
	{
			//Preparing valid event that corresponds to the batch. It will be made invalid in "invalid events"
			return new TestEventSingleToStoreBuilder()
					.id(DUMMY_ID)
					.parentId(batchParentId)
					.name(DUMMY_NAME);
	}
	
	
	@Test(expectedExceptions = {CradleStorageException.class}, expectedExceptionsMessageRegExp = "Test event ID cannot be null")
	public void eventIdMustBeSet() throws CradleStorageException
	{
		new StoredTestEventSingle(new TestEventSingleToStoreBuilder().build());
	}
	
	@Test(expectedExceptions = {CradleStorageException.class}, expectedExceptionsMessageRegExp = "Test event cannot reference itself")
	public void selfReference() throws CradleStorageException
	{
		StoredTestEventId eventId = new StoredTestEventId(BOOK, SCOPE, START_TIMESTAMP, "A");
		new StoredTestEventSingle(new TestEventSingleToStoreBuilder().id(eventId).parentId(eventId).build());
	}
	
	@Test(expectedExceptions = {CradleStorageException.class}, expectedExceptionsMessageRegExp = "Batch must have a parent")
	public void batchParentMustBeSet() throws CradleStorageException
	{
		new StoredTestEventBatch(batchSettingsBuilder
				.id(DUMMY_ID)
				.build());
	}
	
	@Test(expectedExceptions = {CradleStorageException.class}, expectedExceptionsMessageRegExp = "Event .* must have a parent.*")
	public void parentMustBeSet() throws CradleStorageException
	{
		batch.addTestEvent(eventBuilder
				.id(DUMMY_ID)
				.name(DUMMY_NAME)
				.build());
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
	
	@Test(expectedExceptions = {CradleStorageException.class}, expectedExceptionsMessageRegExp = "Test event cannot reference itself")
	public void selfReferenceInBatch() throws CradleIdException, CradleStorageException
	{
		StoredTestEventId eventId = new StoredTestEventId(BOOK, SCOPE, START_TIMESTAMP, "1");
		batch.addTestEvent(eventBuilder.id(eventId).parentId(eventId).build());
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
	
	@Test(dataProvider = "invalid events",
			expectedExceptions = {CradleStorageException.class})
	public void eventValidation(TestEventSingleToStore event, String errorMessage) throws CradleStorageException
	{
		try
		{
			batch.addTestEvent(event);
		}
		catch (CradleStorageException e)
		{
			Assert.assertTrue(e.getMessage().contains(errorMessage), "error contains '"+errorMessage+"'");
			throw e;
		}
	}
}

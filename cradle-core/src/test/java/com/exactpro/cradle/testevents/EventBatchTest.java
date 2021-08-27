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

package com.exactpro.cradle.testevents;

import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import org.testng.asserts.SoftAssert;

import static com.exactpro.cradle.testevents.EventSingleTest.*;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;

import com.exactpro.cradle.BookId;
import com.exactpro.cradle.Direction;
import com.exactpro.cradle.TestUtils;
import com.exactpro.cradle.messages.StoredMessageId;
import com.exactpro.cradle.utils.CradleIdException;
import com.exactpro.cradle.utils.CradleStorageException;
import com.exactpro.cradle.utils.TestEventUtils;

public class EventBatchTest
{
	private TestEventSingleToStoreBuilder eventBuilder = TestEventToStore.singleBuilder();
	private TestEventBatchToStoreBuilder batchBuilder = TestEventToStore.batchBuilder();
	private StoredTestEventId batchId = new StoredTestEventId(BOOK, SCOPE, START_TIMESTAMP, UUID.randomUUID().toString());
	private TestEventBatchToStore batch;
	
	@BeforeMethod
	public void prepareBatch() throws CradleStorageException
	{
		batch = batchBuilder
				.id(batchId)
				.parentId(batchParentId)
				.build();
	}
	
	@DataProvider(name = "batch invalid events")
	public Object[][] batchInvalidEvents() throws CradleStorageException
	{
		Object[][] batchEvents = new Object[][]
				{
					{validEvent().parentId(null),                                                                      //No parent ID
								"must have a parent"},
					{validEvent().id(new BookId(BOOK.getName()+"1"), SCOPE, START_TIMESTAMP, DUMMY_NAME)               //Different book
							.parentId(new StoredTestEventId(new BookId(BOOK.getName()+"1"), 
									SCOPE, START_TIMESTAMP, "Parent_"+DUMMY_NAME)), 
							"events of book"},
					{validEvent().id(BOOK, SCOPE+"1", START_TIMESTAMP, DUMMY_NAME),                                    //Different scope
							"events of scope"},
					{validEvent().id(BOOK, SCOPE, START_TIMESTAMP.minusMillis(5000), DUMMY_NAME),                      //Early timestamp
							"start timestamp"},
					{validEvent().parentId(new StoredTestEventId(BOOK, SCOPE, START_TIMESTAMP, "wrong_parent")),       //Different parent ID
							"parent"},
					{validEvent().parentId(null),                                                                      //No parent
							"must have a parent"}
				};
		
		return Stream.of(new EventSingleTest().invalidEvents(), batchEvents)
				.flatMap(Arrays::stream)
				.toArray(Object[][]::new);
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
		event.addTestEvent(eventBuilder.id(BOOK, SCOPE, START_TIMESTAMP.plusMillis(3500), ID_VALUE)
				.name(DUMMY_NAME)
				.parentId(batch.getParentId())
				.endTimestamp(START_TIMESTAMP.plusMillis(5000))
				.messages(messages1)
				.success(true)
				.build());
		
		Set<StoredMessageId> messages2 = Collections.singleton(new StoredMessageId(BOOK, "Session2", Direction.SECOND, Instant.EPOCH, 2));
		event.addTestEvent(eventBuilder.id(BOOK, SCOPE, START_TIMESTAMP, ID_VALUE+"1")
				.name(DUMMY_NAME)
				.parentId(batch.getParentId())
				.messages(messages2)
				.success(false)
				.build());
		
		event.addTestEvent(eventBuilder.id(BOOK, SCOPE, START_TIMESTAMP, ID_VALUE+"2")
				.name(DUMMY_NAME)
				.parentId(batch.getParentId())
				.success(false)
				.build());
		
		StoredTestEventBatch stored = new StoredTestEventBatch(event, null);
		
		EventTestUtils.assertEvents(stored, event);
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
		batch.addTestEvent(eventBuilder.id(BOOK, SCOPE, START_TIMESTAMP, "2")
				.name(DUMMY_NAME)
				.parentId(batch.getParentId())
				.build());
		batch.addTestEvent(eventBuilder.id(BOOK, SCOPE, START_TIMESTAMP, "3")
				.name(DUMMY_NAME)
				.parentId(parentId)
				.build());
		batch.addTestEvent(eventBuilder.id(BOOK, SCOPE, START_TIMESTAMP, "4")
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
		batch.addTestEvent(eventBuilder.id(BOOK, SCOPE, START_TIMESTAMP, "2")
				.name(DUMMY_NAME)
				.parentId(batch.getId())
				.build());
	}
	
	@Test
	public void childrenAligned() throws CradleStorageException
	{
		BatchedStoredTestEvent parentEvent = batch.addTestEvent(eventBuilder
						.id(BOOK, SCOPE, START_TIMESTAMP, "1")
						.name(DUMMY_NAME)
						.parentId(batch.getParentId())
						.build()),
				childEvent = batch.addTestEvent(eventBuilder
						.id(BOOK, SCOPE, START_TIMESTAMP, "2")
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
						.id(BOOK, SCOPE, START_TIMESTAMP, "1")
						.name(DUMMY_NAME)
						.parentId(batch.getParentId())
						.build()),
				childEvent = batch.addTestEvent(eventBuilder
						.id(BOOK, SCOPE, START_TIMESTAMP, "2")
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
			TestUtils.handleException(e, errorMessage);
		}
	}
	
	@Test
	public void batchEventMessagesAreIndependent() throws CradleStorageException
	{
		TestEventSingleToStore event = validEvent().success(true).message(new StoredMessageId(BOOK, "Session1", Direction.FIRST, START_TIMESTAMP, 1)).build();
		BatchedStoredTestEvent stored = batch.addTestEvent(event);
		
		StoredMessageId newMessage = new StoredMessageId(BOOK, "Session2", Direction.SECOND, START_TIMESTAMP, 2);
		event.getMessages().add(newMessage);
		
		Assert.assertFalse(stored.getMessages().contains(newMessage), "messages in batched event contain new message");
	}
	
	@Test
	public void storedBatchIsIndependent() throws CradleStorageException
	{
		Instant end = START_TIMESTAMP.plusMillis(5000);
		TestEventSingleToStore event = validEvent()
				.success(true)
				.endTimestamp(end)
				.message(new StoredMessageId(BOOK, "Session1", Direction.FIRST, START_TIMESTAMP, 1))
				.build();
		batch.addTestEvent(event);
		StoredTestEventBatch stored = StoredTestEvent.batch(batch, null);
		
		StoredMessageId newMessage = new StoredMessageId(BOOK, "Session2", Direction.SECOND, START_TIMESTAMP, 2);
		event.getMessages().add(newMessage);
		
		StoredMessageId newMessage2 = new StoredMessageId(BOOK, "Session3", Direction.FIRST, START_TIMESTAMP, 3);
		TestEventSingleToStore event2 = validEvent()
				.id(BOOK, SCOPE, START_TIMESTAMP, ID_VALUE+"_2")
				.success(false)
				.endTimestamp(end.plusMillis(1000))
				.message(newMessage2)
				.build();
		batch.addTestEvent(event2);
		
		SoftAssert soft = new SoftAssert();
		soft.assertNull(stored.getTestEvent(event2.getId()), "event added to batch after storing");
		soft.assertFalse(stored.getMessages().contains(newMessage), "messages in stored event contain new message");
		soft.assertFalse(stored.getMessages().contains(newMessage2), "messages in stored event contain new message 2");
		soft.assertTrue(stored.isSuccess());
		soft.assertEquals(stored.getEndTimestamp(), end);
		soft.assertAll();
	}
}

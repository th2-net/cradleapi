/*
 * Copyright 2021-2022 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.cradle.messages.GroupedMessageBatchToStore;
import com.exactpro.cradle.messages.MessageToStore;
import com.exactpro.cradle.messages.MessageToStoreBuilder;
import com.exactpro.cradle.messages.StoredMessage;
import com.exactpro.cradle.testevents.*;
import com.exactpro.cradle.utils.CradleStorageException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.time.Instant;

import static org.testng.Assert.assertEquals;


public class CradleStorageTest
{
	private final String BOOK = "Book1",
			SCOPE = "Scope1",
			EVENT_ID = "Event1";
	private final BookId BOOK_ID = new BookId(BOOK);
	private final Instant BOOK_START = Instant.parse("2022-11-03T10:15:30.00Z");
	private final Instant PAGE1_START = BOOK_START.plusSeconds(1);
	private final Instant PAGE2_START = PAGE1_START.plusSeconds(60);
	private final Instant PAGE3_START = PAGE2_START.plusSeconds(100);
	private final StoredTestEventId DUMMY_EVENT_ID = new StoredTestEventId(BOOK_ID, SCOPE, BOOK_START, EVENT_ID);

	private final long storeActionRejectionThreshold = new CoreStorageSettings().calculateStoreActionRejectionThreshold();
	
	private CradleStorage storage;
	private MessageToStoreBuilder builder;
	
	@BeforeMethod
	public void prepare() throws CradleStorageException, IOException
	{
		storage = new DummyCradleStorage();
		storage.init(false);
		storage.addBook(new BookToAdd(BOOK, BOOK_START));

		BookId bookId = new BookId(BOOK);
		BookInfo bookInfo = storage.getBookCache().getBook(bookId);
		bookInfo.addPage(createPage(bookId, "page1", PAGE1_START, PAGE2_START));
		bookInfo.addPage(createPage(bookId, "page2", PAGE2_START, PAGE3_START));
		bookInfo.addPage(createPage(bookId, "page3", PAGE3_START, null));

		builder = new MessageToStoreBuilder();

	}

	private PageInfo createPage(BookId bookId, String name, Instant start, Instant end) {
		PageId pageId = new PageId(bookId, name);
		return new PageInfo(pageId, start, end, null, null, null);
	}

	private MessageToStore createMessage(BookId book, String sessionAlias, Direction direction, int sequence, Instant timestamp) throws CradleStorageException {
		return builder.bookId(book)
				.sessionAlias(sessionAlias)
				.direction(direction)
				.sequence(sequence)
				.timestamp(timestamp)
				.content("content".getBytes())
				.build();
	}

	@DataProvider(name = "invalid events")
	public Object[][] invalidEvents() throws CradleStorageException
	{
		return new Object[][]
				{
					{validEvent()
							.id(new BookId(BOOK+"_X"), SCOPE, BOOK_START, EVENT_ID),          //Unknown book
							"unknown"},
					{validEvent()
							.id(new BookId(BOOK), SCOPE, Instant.EPOCH, EVENT_ID),                 //Too early
							"no page for timestamp"},
					{validEvent().endTimestamp(BOOK_START.minusMillis(5000)),             //End before start
							"cannot end sooner than it started"}
				};
	}
	
	public TestEventSingleToStoreBuilder validEvent()
	{
		//Preparing valid event. It will be made invalid in "invalid events"
		return new TestEventSingleToStoreBuilder(storeActionRejectionThreshold)
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

	private void addMessages(GroupedMessageBatchToStore batch, MessageToStore[] messages) throws CradleStorageException {
		for (MessageToStore page1Message : messages) {
			batch.addMessage(page1Message);
		}
	}

	private void verifyMessageBatch(GroupedMessageBatchToStore batch, MessageToStore[] messages) {
		assertEquals(batch.getMessageCount(), messages.length);
		var batchedMessages = batch.getMessages();

		int i = 0;
		for (StoredMessage message : batchedMessages) {
			assertEquals(message.getId(), messages[i].getId());
			assertEquals(message.getSessionAlias(), messages[i].getSessionAlias());
			assertEquals(message.getDirection(), messages[i].getDirection());
			assertEquals(message.getSequence(), messages[i].getSequence());
			assertEquals(message.getTimestamp(), messages[i].getTimestamp());
			i++;
		}
	}

	@Test
	public void testSinglePageMessageBatch() throws CradleStorageException {

		String groupName = "test-group";
		BookId bookId = new BookId(BOOK);

		GroupedMessageBatchToStore batch = new GroupedMessageBatchToStore(groupName, 1_000_000, storeActionRejectionThreshold);

		MessageToStore[] messages = new MessageToStore[]{
				createMessage(bookId, "session-1", Direction.FIRST, 1, PAGE1_START.plusSeconds(1)),
				createMessage(bookId, "session-1", Direction.FIRST, 2, PAGE1_START.plusSeconds(2)),
				createMessage(bookId, "session-1", Direction.SECOND, 1, PAGE1_START.plusSeconds(2)),
				createMessage(bookId, "session-2", Direction.FIRST, 2, PAGE1_START.plusSeconds(3))
		};
		addMessages(batch, messages);

		var batches = storage.paginateBatch(batch);
		assertEquals(batches.size(), 1);
		verifyMessageBatch(batches.get(0).getKey(), messages);
	}

	@Test
	public void testMultiPageMessageBatch() throws CradleStorageException {

		String groupName = "test-group";
		BookId bookId = new BookId(BOOK);

		GroupedMessageBatchToStore batch = new GroupedMessageBatchToStore(groupName, 1_000_000, storeActionRejectionThreshold);

		// page 1
		MessageToStore[] page1Messages = new MessageToStore[]{
				createMessage(bookId, "session-1", Direction.FIRST, 1, PAGE1_START.plusSeconds(1)),
				createMessage(bookId, "session-1", Direction.FIRST, 2, PAGE1_START.plusSeconds(2)),
				createMessage(bookId, "session-1", Direction.SECOND, 1, PAGE1_START.plusSeconds(2)),
				createMessage(bookId, "session-2", Direction.FIRST, 2, PAGE1_START.plusSeconds(3))
		};
		addMessages(batch, page1Messages);

		// page 2
		MessageToStore[] page2Messages = new MessageToStore[]{
				createMessage(bookId, "session-1", Direction.SECOND, 3, PAGE2_START),
				createMessage(bookId, "session-2", Direction.FIRST, 3, PAGE2_START.plusSeconds(3))
		};
		addMessages(batch, page2Messages);

		// page 3
		MessageToStore[] page3Messages = new MessageToStore[]{
				createMessage(bookId, "session-1", Direction.FIRST, 3, PAGE3_START)
		};
		addMessages(batch, page3Messages);

		var batches = storage.paginateBatch(batch);
		assertEquals(batches.size(), 3);
		verifyMessageBatch(batches.get(0).getKey(), page1Messages);
		verifyMessageBatch(batches.get(1).getKey(), page2Messages);
		verifyMessageBatch(batches.get(2).getKey(), page3Messages);
	}


	private void verifyEventBatch(TestEventBatchToStore actual, TestEventBatchToStore expected) {
		assertEquals(actual.getTestEventsCount(), expected.getTestEventsCount());
		assertEquals(actual.getId(), expected.getId());
		assertEquals(actual.getName(), expected.getName());
		assertEquals(actual.getParentId(), expected.getParentId());
		assertEquals(actual.getType(), expected.getType());
		assertEquals(actual.getBatchSize(), expected.getBatchSize());
	}

	private TestEventSingleToStore createEvent(String name, Instant start, Instant end, StoredTestEventId parentId, boolean success)
			throws CradleStorageException
	{
		return new TestEventSingleToStoreBuilder(storeActionRejectionThreshold)
							.id(new StoredTestEventId(BOOK_ID, "test-scope", start, name + "-id"))
							.name(name)
							.type(name + "-type")
							.parentId(parentId)
							.success(success)
							.content((name + "-data").getBytes())
							.endTimestamp(end)
							.build();
	}

	private void verifyTestEvent(TestEventBatchToStore actual, TestEventBatchToStore expected, String name, Instant expectedTimestamp) {
		BatchedStoredTestEvent aEvent = actual.getTestEvents().stream().filter((e) -> name.equals(e.getName())).findFirst().get();
		BatchedStoredTestEvent eEvent = expected.getTestEvents().stream().filter((e) -> name.equals(e.getName())).findFirst().get();

		StoredTestEventId aId = aEvent.getId();
		StoredTestEventId eId = eEvent.getId();
		assertEquals(aId.getBookId(), eId.getBookId());
		assertEquals(aId.getScope(), eId.getScope());
		assertEquals(aId.getStartTimestamp(), expectedTimestamp == null ? eId.getStartTimestamp() : expectedTimestamp);

		assertEquals(aEvent.getBatchId(), eEvent.getBatchId());
		assertEquals(aEvent.getType(), eEvent.getType());
		assertEquals(aEvent.getParentId(), aEvent.getParentId());
		assertEquals(aEvent.isSuccess(), eEvent.isSuccess());
		assertEquals(aEvent.getContent(), eEvent.getContent());
		assertEquals(aEvent.getEndTimestamp(), eEvent.getEndTimestamp());
	}


	@Test
	public void testMultiPageEventBatch() throws CradleStorageException {

		BookId bookId = new BookId(BOOK);

		StoredTestEventId batchId = new StoredTestEventId(bookId, "test-scope", PAGE1_START, "batch-id");
		TestEventBatchToStore batch = new TestEventBatchToStoreBuilder(1_000_000, storeActionRejectionThreshold)
				.id(batchId)
				.name("test-batch")
				.parentId(new StoredTestEventId(bookId, "test-scope", PAGE1_START, "batch-parent-id"))
				.type("batch-type")
				.build();

		// page1
		TestEventSingleToStore e1 = createEvent("evt-1", PAGE1_START.plusMillis(10), null, batch.getParentId(), true);
		batch.addTestEvent(e1);

		TestEventSingleToStore e2 = createEvent("evt-2", PAGE1_START.plusMillis(15), PAGE1_START.plusSeconds(2), e1.getId(), false);
		batch.addTestEvent(e2);

		// page2
		TestEventSingleToStore e3 = createEvent("evt-3", PAGE2_START.plusMillis(20), PAGE3_START.plusSeconds(5), batch.getParentId(), true);
		batch.addTestEvent(e3);

		TestEventSingleToStore e4 = createEvent("evt-4", PAGE2_START.plusMillis(20), null, e1.getId(), true);
		batch.addTestEvent(e4);

		// page3
		TestEventSingleToStore e5 = createEvent("evt-5", PAGE3_START.plusMillis(30), PAGE3_START.plusSeconds(2), batch.getParentId(), true);
		batch.addTestEvent(e5);

		TestEventSingleToStore e6 = createEvent("evt-6", PAGE3_START.plusMillis(130), null, e4.getId(), false);
		batch.addTestEvent(e6);

		TestEventSingleToStore e7 = createEvent("evt-7", PAGE3_START.plusMillis(230), null, e5.getId(), true);
		batch.addTestEvent(e7);


		TestEventBatchToStore alignedBatch = (TestEventBatchToStore) storage.alignEventTimestampsToPage(batch, storage.findPage(BOOK_ID, e1.getStartTimestamp()));
		verifyEventBatch(alignedBatch, batch);
		Instant end = PAGE2_START.minusNanos(1);
		verifyTestEvent(alignedBatch, batch, "evt-1", null);
		verifyTestEvent(alignedBatch, batch, "evt-2", null);
		verifyTestEvent(alignedBatch, batch, "evt-3", end);
		verifyTestEvent(alignedBatch, batch, "evt-4", end);
		verifyTestEvent(alignedBatch, batch, "evt-5", end);
		verifyTestEvent(alignedBatch, batch, "evt-6", end);
		verifyTestEvent(alignedBatch, batch, "evt-7", end);
	}


	@Test
	public void testSinglePageEventBatch() throws CradleStorageException {

		BookId bookId = new BookId(BOOK);

		StoredTestEventId batchId = new StoredTestEventId(bookId, "test-scope", PAGE1_START, "batch-id");
		TestEventBatchToStore batch = new TestEventBatchToStoreBuilder(1_000_000, storeActionRejectionThreshold)
				.id(batchId)
				.name("test-batch")
				.parentId(new StoredTestEventId(bookId, "test-scope", PAGE1_START, "batch-parent-id"))
				.type("batch-type")
				.build();

		// page1
		TestEventSingleToStore e1 = createEvent("evt-1", PAGE1_START.plusMillis(10), null, batch.getParentId(), true);
		batch.addTestEvent(e1);

		TestEventSingleToStore e2 = createEvent("evt-2", PAGE1_START.plusMillis(15), PAGE1_START.plusSeconds(2), e1.getId(), false);
		batch.addTestEvent(e2);

		// page2
		TestEventSingleToStore e3 = createEvent("evt-3", PAGE1_START.plusMillis(30), PAGE3_START.plusSeconds(5), batch.getParentId(), true);
		batch.addTestEvent(e3);

		TestEventSingleToStore e4 = createEvent("evt-4", PAGE1_START.plusMillis(32), null, e1.getId(), true);
		batch.addTestEvent(e4);

		// page3
		TestEventSingleToStore e5 = createEvent("evt-5", PAGE1_START.plusMillis(190), PAGE3_START.plusSeconds(2), batch.getParentId(), true);
		batch.addTestEvent(e5);

		TestEventSingleToStore e6 = createEvent("evt-6", PAGE1_START.plusMillis(130), null, e4.getId(), false);
		batch.addTestEvent(e6);

		TestEventSingleToStore e7 = createEvent("evt-7", PAGE1_START.plusMillis(230), null, e5.getId(), true);
		batch.addTestEvent(e7);


		TestEventBatchToStore alignedBatch = (TestEventBatchToStore) storage.alignEventTimestampsToPage(batch, storage.findPage(BOOK_ID, e1.getStartTimestamp()));
		verifyEventBatch(alignedBatch, batch);
		verifyTestEvent(alignedBatch, batch, "evt-1", null);
		verifyTestEvent(alignedBatch, batch, "evt-2", null);
		verifyTestEvent(alignedBatch, batch, "evt-3", null);
		verifyTestEvent(alignedBatch, batch, "evt-4", null);
		verifyTestEvent(alignedBatch, batch, "evt-5", null);
		verifyTestEvent(alignedBatch, batch, "evt-6", null);
		verifyTestEvent(alignedBatch, batch, "evt-7", null);
	}
}
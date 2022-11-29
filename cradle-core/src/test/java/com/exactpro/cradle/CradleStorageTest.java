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
import com.exactpro.cradle.testevents.StoredTestEventId;
import com.exactpro.cradle.testevents.TestEventSingleToStoreBuilder;
import com.exactpro.cradle.utils.CradleStorageException;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.time.Instant;


public class CradleStorageTest
{
	private final String BOOK = "Book1",
			SCOPE = "Scope1",
			EVENT_ID = "Event1";
	private final BookId BOOK_ID = new BookId(BOOK);
	private final Instant BOOK_START = Instant.parse("2022-11-03T10:15:30.00Z");
	private final Instant PAGE2_START = BOOK_START.plusSeconds(60);
	private final Instant PAGE3_START = PAGE2_START.plusSeconds(100);
	private final StoredTestEventId DUMMY_EVENT_ID = new StoredTestEventId(BOOK_ID, SCOPE, BOOK_START, EVENT_ID);
	
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
		bookInfo.addPage(createPage(bookId, "page1", BOOK_START, PAGE2_START));
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

	private void addMessages(GroupedMessageBatchToStore batch, MessageToStore[] messages) throws CradleStorageException {
		for (MessageToStore page1Message : messages) {
			batch.addMessage(page1Message);
		}
	}

	private void verifyBatch(GroupedMessageBatchToStore batch, MessageToStore[] messages) {
		Assert.assertEquals(batch.getMessageCount(), messages.length);
		var batchedMessages = batch.getMessages();

		int i = 0;
		for (StoredMessage message : batchedMessages) {
			Assert.assertEquals(message.getId(), messages[i].getId());
			Assert.assertEquals(message.getSessionAlias(), messages[i].getSessionAlias());
			Assert.assertEquals(message.getDirection(), messages[i].getDirection());
			Assert.assertEquals(message.getSequence(), messages[i].getSequence());
			Assert.assertEquals(message.getTimestamp(), messages[i].getTimestamp());
			i++;
		}
	}

	@Test
	public void testSinglePageBatch() throws CradleStorageException {

		String groupName = "test-group";
		BookId bookId = new BookId(BOOK);

		GroupedMessageBatchToStore batch = new GroupedMessageBatchToStore(groupName, 1_000_000);

		MessageToStore[] messages = new MessageToStore[]{
				createMessage(bookId, "session-1", Direction.FIRST, 1, BOOK_START.plusSeconds(1)),
				createMessage(bookId, "session-1", Direction.FIRST, 2, BOOK_START.plusSeconds(2)),
				createMessage(bookId, "session-1", Direction.SECOND, 1, BOOK_START.plusSeconds(2)),
				createMessage(bookId, "session-2", Direction.FIRST, 2, BOOK_START.plusSeconds(3))
		};
		addMessages(batch, messages);

		var batches = storage.paginateBatch(batch);
		Assert.assertEquals(batches.size(), 1);
		verifyBatch(batches.get(0).getKey(), messages);
	}

	@Test
	public void testMultiPageBatch() throws CradleStorageException {

		String groupName = "test-group";
		BookId bookId = new BookId(BOOK);

		GroupedMessageBatchToStore batch = new GroupedMessageBatchToStore(groupName, 1_000_000);

		// page 1
		MessageToStore[] page1Messages = new MessageToStore[]{
				createMessage(bookId, "session-1", Direction.FIRST, 1, BOOK_START.plusSeconds(1)),
				createMessage(bookId, "session-1", Direction.FIRST, 2, BOOK_START.plusSeconds(2)),
				createMessage(bookId, "session-1", Direction.SECOND, 1, BOOK_START.plusSeconds(2)),
				createMessage(bookId, "session-2", Direction.FIRST, 2, BOOK_START.plusSeconds(3))
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
		Assert.assertEquals(batches.size(), 3);
		verifyBatch(batches.get(0).getKey(), page1Messages);
		verifyBatch(batches.get(1).getKey(), page2Messages);
		verifyBatch(batches.get(2).getKey(), page3Messages);
	}
}

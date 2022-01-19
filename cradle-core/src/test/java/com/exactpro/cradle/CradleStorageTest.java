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
	private final Instant START_TIMESTAMP = Instant.now();
	private final StoredTestEventId DUMMY_EVENT_ID = new StoredTestEventId(BOOK_ID, SCOPE, START_TIMESTAMP, EVENT_ID);
	
	private CradleStorage storage;
	
	@BeforeMethod
	public void prepare() throws CradleStorageException, IOException
	{
		storage = new DummyCradleStorage();
		storage.init(false);
		storage.addBook(new BookToAdd(BOOK, START_TIMESTAMP, PAGE));
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
							"no page for timestamp"},
					{validEvent().endTimestamp(START_TIMESTAMP.minusMillis(5000)),             //End before start
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

	@Test(expectedExceptions = {CradleStorageException.class}, expectedExceptionsMessageRegExp = "Invalid book name.*")
	public void invalidBookName() throws IOException, CradleStorageException
	{
		storage.addBook(new BookToAdd("book%%", Instant.now(), "page1"));
	}

	@Test(expectedExceptions = {CradleStorageException.class}, expectedExceptionsMessageRegExp = "Invalid book name.*")
	public void invalidBookName2() throws IOException, CradleStorageException
	{
		storage.addBook(new BookToAdd(null, Instant.now(), "page1"));
	}

	@Test(expectedExceptions = {CradleStorageException.class}, expectedExceptionsMessageRegExp = "Invalid book name.*")
	public void invalidBookName3() throws IOException, CradleStorageException
	{
		storage.addBook(new BookToAdd("_book", Instant.now(), "page1"));
	}

	@Test(expectedExceptions = {CradleStorageException.class}, expectedExceptionsMessageRegExp = "Invalid book name.*")
	public void invalidBookName4() throws IOException, CradleStorageException
	{
		storage.addBook(new BookToAdd("bo ok", Instant.now(), "page1"));
	}

	@Test
	public void validBookName() throws IOException, CradleStorageException
	{
		storage.addBook(new BookToAdd("book", Instant.now(), "page1"));
		storage.addBook(new BookToAdd("book_2", Instant.now(), "page1"));
		storage.addBook(new BookToAdd("BOOK3", Instant.now(), "page1"));
		storage.addBook(new BookToAdd("4book", Instant.now(), "page1_"));
	}

	@Test(expectedExceptions = {CradleStorageException.class}, expectedExceptionsMessageRegExp = "Invalid first page name.*")
	public void invalidFirstPageName() throws IOException, CradleStorageException
	{
		storage.addBook(new BookToAdd("book", Instant.now(), "_page1"));
	}

	@Test(expectedExceptions = {CradleStorageException.class}, expectedExceptionsMessageRegExp = "Invalid first page name.*")
	public void invalidFirstPageName2() throws IOException, CradleStorageException
	{
		storage.addBook(new BookToAdd("book", Instant.now(), "pag%%e1"));
	}

	@Test(expectedExceptions = {CradleStorageException.class}, expectedExceptionsMessageRegExp = "Invalid page name.*")
	public void invalidPageName() throws IOException, CradleStorageException
	{
		storage.addPage(BOOK_ID, "_page1", Instant.now(), "comment");
	}

	@Test(expectedExceptions = {CradleStorageException.class}, expectedExceptionsMessageRegExp = "Invalid page name.*")
	public void invalidPageName2() throws IOException, CradleStorageException
	{
		storage.addPage(BOOK_ID, "pa#ge1", Instant.now(), "comment");
	}

	@Test(expectedExceptions = {CradleStorageException.class}, expectedExceptionsMessageRegExp = "Invalid page name.*")
	public void invalidPageName3() throws IOException, CradleStorageException
	{
		storage.addPage(BOOK_ID, "pa ge1", Instant.now(), "comment");
	}

	@Test(expectedExceptions = {CradleStorageException.class}, expectedExceptionsMessageRegExp = "Invalid page name.*")
	public void invalidPageName4() throws IOException, CradleStorageException
	{
		storage.addPage(BOOK_ID, null, Instant.now(), "comment");
	}

	@Test
	public void validPageName() throws IOException, CradleStorageException
	{
		storage.addPage(BOOK_ID, "page", Instant.now(), "comment");
		storage.addPage(BOOK_ID, "page2_", Instant.now(), "comment");
		storage.addPage(BOOK_ID, "page_3", Instant.now(), "comment");
		storage.addPage(BOOK_ID, "4page", Instant.now(), "comment");
	}
}

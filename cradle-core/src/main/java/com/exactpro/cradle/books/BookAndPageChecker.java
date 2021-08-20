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

package com.exactpro.cradle.books;

import java.time.Instant;
import java.util.Map;

import com.exactpro.cradle.utils.CradleStorageException;

public class BookAndPageChecker
{
	private final Map<BookId, BookInfo> books;
	
	public BookAndPageChecker(Map<BookId, BookInfo> books)
	{
		this.books = books;
	}
	
	
	public BookInfo getBook(BookId bookId) throws CradleStorageException
	{
		BookInfo result = books.get(bookId);
		if (result == null)
			throw new CradleStorageException("Book '"+bookId+"' is unknown");
		return result;
	}
	
	public PageInfo getActivePage(BookId bookId) throws CradleStorageException
	{
		BookInfo book = getBook(bookId);
		PageInfo page = book.getActivePage();
		if (page == null)
			throw new CradleStorageException("Book '"+bookId+"' has no active pages");
		return page;
	}
	
	public PageId getActivePageId(BookId bookId) throws CradleStorageException
	{
		return getActivePage(bookId).getId();
	}
	
	
	public void checkPage(PageId pageId, BookId bookFromId) throws CradleStorageException
	{
		BookInfo book = getBook(bookFromId);
		if (!bookFromId.equals(pageId.getBookId()))
			throw new CradleStorageException("Book in ID ("+bookFromId+") doesn't match book of requested page ("+pageId.getBookId()+")");
		if (book.getPage(pageId) == null)
			throw new CradleStorageException("Page '"+pageId+"' is unknown");
	}
	
	public PageInfo checkActivePage(BookId book, Instant timestamp) throws CradleStorageException
	{
		PageInfo activePage = getActivePage(book);
		if (activePage.getStarted().isAfter(timestamp))
			throw new CradleStorageException("Active page '"+activePage.getId()+"' was started after given timestamp ("+timestamp+")");
		return activePage;
	}
	
	public void checkPage(PageId pageId) throws CradleStorageException
	{
		if (getBook(pageId.getBookId()).getPage(pageId) == null)
			throw new CradleStorageException("Page '"+pageId+"' is unknown");
	}
}

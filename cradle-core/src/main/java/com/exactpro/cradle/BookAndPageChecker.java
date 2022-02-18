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

import java.time.Instant;

import com.exactpro.cradle.utils.CradleStorageException;

public class BookAndPageChecker
{
	private BookCache bookCache;
	
	public BookAndPageChecker(BookCache bookCache)
	{
		this.bookCache = bookCache;
	}
	
	
	public BookInfo getBook(BookId bookId) throws CradleStorageException
	{
		BookInfo result = bookCache.getBook(bookId);
		if (result == null)
			throw new CradleStorageException("Book '"+bookId+"' is unknown");
		return result;
	}

	public boolean checkBook (BookId bookId) {
		return bookCache.getBook(bookId) != null;
	}
	
	public PageInfo findPage(BookId bookId, Instant timestamp) throws CradleStorageException
	{
		BookInfo book = getBook(bookId);
		Instant now = Instant.now();
		if (timestamp.isAfter(now))
			throw new CradleStorageException("Timestamp "+timestamp+" is from future, now is "+now);
		PageInfo page = book.findPage(timestamp);
		if (page == null || (page.getEnded() != null && !timestamp.isBefore(page.getEnded())))  //If page.getEnded().equals(timestamp), timestamp is outside of page
			throw new CradleStorageException("Book '"+bookId+"' has no page for timestamp "+timestamp);
		return page;
	}
	
	
	public void checkPage(PageId pageId, BookId bookFromId) throws CradleStorageException
	{
		BookInfo book = getBook(bookFromId);
		if (!bookFromId.equals(pageId.getBookId()))
			throw new CradleStorageException("Requested book ("+bookFromId+") doesn't match book of requested page ("+pageId.getBookId()+")");
		if (book.getPage(pageId) == null)
			throw new CradleStorageException("Page '"+pageId+"' is unknown");
	}
	
	public void checkPage(PageId pageId) throws CradleStorageException
	{
		if (getBook(pageId.getBookId()).getPage(pageId) == null)
			throw new CradleStorageException("Page '"+pageId+"' is unknown");
	}
}

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
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.exactpro.cradle.utils.CradleStorageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BookAndPageChecker
{
	private final static Logger logger = LoggerFactory.getLogger(BookAndPageChecker.class);
	private final BookCache bookCache;
	private ScheduledExecutorService executorService;
	private Long refreshIntervalMillis;

	public BookAndPageChecker(BookCache bookCache)
	{
		this.bookCache = bookCache;
	}

	public BookAndPageChecker (BookCache bookCache, long refreshIntervalMillis) {
		this.bookCache = bookCache;
		this.refreshIntervalMillis = refreshIntervalMillis;
	}

	public void start() {
		if (refreshIntervalMillis == null) {
			logger.warn("{} wasn't created with `refreshIntervalMillis` argument, cached books won't be refreshed on background", getClass().getName());
			return;
		}

		executorService = Executors.newScheduledThreadPool(3);
		logger.debug("Registered refresher task for cached books");
		executorService.scheduleWithFixedDelay(new Refresher(bookCache), refreshIntervalMillis, refreshIntervalMillis, TimeUnit.MILLISECONDS);
	}

	private static class Refresher implements Runnable {

		private final BookCache bookCache;

		Refresher (BookCache bookCache) {
			this.bookCache = bookCache;
		}

		@Override
		public void run() {
			logger.debug("Refreshing books");
			try {
				List<BookInfo> cachedBookInfos = new ArrayList<>(bookCache.getCachedBooks());
				List<BookInfo> oldBookInfos = new ArrayList<>(cachedBookInfos);
				List<BookInfo> updatedBookInfos = new ArrayList<>();
				// Cache should be copied for thread safety
				Collections.copy(oldBookInfos, cachedBookInfos);

				for (BookInfo oldBookInfo : oldBookInfos) {
					try {
						BookInfo newBookInfo = bookCache.loadBook(oldBookInfo.getId());
						if (!oldBookInfo.equals(newBookInfo)) {
							updatedBookInfos.add(newBookInfo);
						}

					} catch (CradleStorageException e) {
						logger.error("Refresher could not get new book info for {}: {}", oldBookInfo.getId().getName(), e.getMessage());
					}
				}

				for (BookInfo updatedBookInfo : updatedBookInfos) {
					logger.debug("Refreshing book {}", updatedBookInfo.getId().getName());
					bookCache.updateCachedBook(updatedBookInfo);
				}


				logger.debug("Refreshed {} books", updatedBookInfos.size());
			} catch (Exception e) {
				/*
				 	Any exceptions should be cached and logged,
				 	task should be executed periodically
				 */
				logger.error("Error while refreshing books in background {}", e.toString());
			}
		}
	}

	public void stop() {
		if (refreshIntervalMillis == null) {
			logger.warn("{} wasn't created with `refreshIntervalMillis` argument, there's nothing to stop", getClass().getName());
			return;
		}

		logger.debug("Refresher executor shutting down");
		executorService.shutdown();
	}

	public BookInfo getBook(BookId bookId) throws CradleStorageException
	{
		return bookCache.getBook(bookId);
	}

	public boolean checkBook (BookId bookId) {
		return bookCache.checkBook(bookId);
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

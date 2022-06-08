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

public class BookManager
{
	private final static Logger logger = LoggerFactory.getLogger(BookManager.class);
	private final BookCache bookCache;
	private ScheduledExecutorService executorService;
	private Long refreshIntervalMillis;

	public BookManager(BookCache bookCache)
	{
		this.bookCache = bookCache;
	}

	public BookManager(BookCache bookCache, long refreshIntervalMillis) {
		this.bookCache = bookCache;
		this.refreshIntervalMillis = refreshIntervalMillis;
	}

	public void start() {
		if (refreshIntervalMillis == null) {
			logger.warn("{} wasn't created with `refreshIntervalMillis` argument, cached books won't be refreshed on background", getClass().getName());
			return;
		}

		executorService = Executors.newScheduledThreadPool(1);
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

				for (BookInfo oldBookInfo : oldBookInfos) {
					try {
						BookInfo newBookInfo = bookCache.loadBook(oldBookInfo.getId());
						if (!oldBookInfo.equals(newBookInfo)) {
							logger.info("Refreshing book {}", oldBookInfo.getId().getName());
							bookCache.updateCachedBook(oldBookInfo);
						}

					} catch (CradleStorageException e) {
						logger.error("Refresher could not get new book info for {}: {}", oldBookInfo.getId().getName(), e.getMessage());
					}
				}
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
}

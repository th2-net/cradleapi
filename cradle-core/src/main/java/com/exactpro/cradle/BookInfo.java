/*
 * Copyright 2021-2024 Exactpro (Exactpro Systems Limited)
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

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReference;

import static com.exactpro.cradle.BookInfoMetrics.CacheName.HOT;
import static com.exactpro.cradle.BookInfoMetrics.CacheName.RANDOM;
import static com.exactpro.cradle.BookInfoMetrics.RequestMethod.FIND;
import static com.exactpro.cradle.BookInfoMetrics.RequestMethod.GET;
import static com.exactpro.cradle.BookInfoMetrics.RequestMethod.NEXT;
import static com.exactpro.cradle.BookInfoMetrics.RequestMethod.PREVIOUS;
import static java.util.Collections.unmodifiableMap;
import static java.util.Collections.unmodifiableNavigableMap;

/**
 * Information about a book
 */
public class BookInfo
{
	private static final Logger LOGGER = LoggerFactory.getLogger(BookInfo.class);
	private static final BookInfoMetrics METRICS = new BookInfoMetrics();
	private static final int HOT_CACHE_SIZE = 2;
	private static final int SECONDS_IN_DAY = 60 * 60 * 24;
	private static final int MILLISECONDS_IN_DAY = SECONDS_IN_DAY * 1_000;
	private static final IPageInterval EMPTY_PAGE_INTERVAL = new EmptyPageInterval();
	private final BookId id;
	private final String fullName,
			desc;
	private final Instant created;

	private final LoadingCache<Long, IPageInterval> hotCache;
	private final LoadingCache<Long, IPageInterval> randomAccessCache;

    // AtomicInitializer call initialize method again if previous value is null
	private final AtomicReference<PageInfo> firstPage = new AtomicReference<>();
	private final PagesLoader pagesLoader;
	private final PageLoader firstPageLoader;
	private final PageLoader lastPageLoader;

	public BookInfo(BookId id,
					String fullName,
					String desc,
					Instant created,
					int cacheSize,
					PagesLoader pagesLoader,
					PageLoader firstPageLoader,
					PageLoader lastPageLoader) {
		this.id = id;
		this.fullName = fullName;
		this.desc = desc;
		this.created = created;
		this.pagesLoader = pagesLoader;
		this.firstPageLoader = firstPageLoader;
		this.lastPageLoader = lastPageLoader;

		this.hotCache = Caffeine.newBuilder()
				.maximumSize(HOT_CACHE_SIZE)
				.removalListener((epochDay, pageInterval, cause) -> METRICS.incInvalidate(id.getName(), HOT, cause))
				.build(epochDay -> {
					IPageInterval pageInterval = createPageInterval(HOT, epochDay);
					if (pageInterval != null) {
						METRICS.incLoads(id.getName(), pageInterval.getCacheName(), pageInterval.size());
					}
					return pageInterval;
				});

		this.randomAccessCache = Caffeine.newBuilder()
				.maximumSize(cacheSize)
				.removalListener((epochDay, pageInterval, cause) -> METRICS.incInvalidate(id.getName(), RANDOM, cause))
				.build(epochDay -> {
					IPageInterval pageInterval = createPageInterval(RANDOM, epochDay);
					if (pageInterval != null) {
						METRICS.incLoads(id.getName(), pageInterval.getCacheName(), pageInterval.size());
					}
					return pageInterval;
				});
	}

	public BookId getId()
	{
		return id;
	}
	
	public String getFullName()
	{
		return fullName;
	}
	
	public String getDesc()
	{
		return desc;
	}
	
	public Instant getCreated()
	{
		return created;
	}

	/**
	 * Requests pages from {@link PagesLoader} with book id and null start/end
	 * @return all ordered pages related to book id
	 */
	public Collection<PageInfo> getPages()
	{
		return pagesLoader.load(id, null, null);
	}

	public @Nullable PageInfo getFirstPage()
	{
		PageInfo result = firstPage.get();

		if (result == null) {
			result = firstPageLoader.load(id);
			if (!firstPage.compareAndSet(null, result)) {
				// another thread has initialized the reference
				result = firstPage.get();
			}
		}

		return result;
    }
	
	public @Nullable PageInfo getLastPage()
	{
		return lastPageLoader.load(id);
	}
	
	public PageInfo getPage(PageId pageId)
	{
		IPageInterval pageInterval = getPageInterval(pageId.getStart());
		METRICS.incRequest(id.getName(), pageInterval.getCacheName(), GET);
		return pageInterval.get(pageId);
	}
	
	public PageInfo findPage(Instant timestamp)
	{
		IPageInterval pageInterval = getPageInterval(timestamp);
		METRICS.incRequest(id.getName(), pageInterval.getCacheName(), FIND);
		return pageInterval.find(timestamp);
	}

	public PageInfo getNextPage(Instant startTimestamp)
	{
		long epochDate = getEpochDay(startTimestamp);
		IPageInterval pageInterval = getPageInterval(epochDate);
		METRICS.incRequest(id.getName(), pageInterval.getCacheName(), NEXT);
		PageInfo currentInterval = pageInterval.next(startTimestamp);
		if (currentInterval != null) {
			return currentInterval;
		}
		pageInterval = getPageInterval(epochDate + 1);
		METRICS.incRequest(id.getName(), pageInterval.getCacheName(), NEXT);
		return pageInterval.next(startTimestamp);
	}
	
	public PageInfo getPreviousPage(Instant startTimestamp)
	{
		long epochDate = getEpochDay(startTimestamp);
		IPageInterval pageInterval = getPageInterval(epochDate);
		METRICS.incRequest(id.getName(), pageInterval.getCacheName(), PREVIOUS);
		PageInfo currentInterval = pageInterval.previous(startTimestamp);
		if (currentInterval != null) {
			return currentInterval;
		}
		pageInterval = getPageInterval(epochDate - 1);
		METRICS.incRequest(id.getName(), pageInterval.getCacheName(), PREVIOUS);
		return pageInterval.previous(startTimestamp);
	}

	void invalidate() {
		firstPage.set(null);
		hotCache.invalidateAll();
		randomAccessCache.invalidateAll();
	}

	void invalidate(Instant timestamp) {
		long epochDay = getEpochDay(timestamp);
		invalidate(epochDay);
	}

	void invalidate(Collection<Instant> timestamps) {
		timestamps.stream()
				.map(BookInfo::getEpochDay)
				.distinct()
				.forEach(this::invalidate);
	}

	void removePage(PageId pageId) {
		invalidate(pageId.getStart());
	}
	
	void addPage(PageInfo page) {
		invalidate(page.getId().getStart());
	}

	private void invalidate(long epochDay) {
		hotCache.invalidate(epochDay);
		randomAccessCache.invalidate(epochDay);
	}

	private IPageInterval getPageInterval(long epochDate) {
		long currentEpochDate = currentEpochDay();
		IPageInterval pageInterval = currentEpochDate - epochDate < 2
				? hotCache.get(epochDate)
				: randomAccessCache.get(epochDate);
		return pageInterval != null
				? pageInterval
				: EMPTY_PAGE_INTERVAL;
	}

	private IPageInterval getPageInterval(Instant timestamp) {
		long epochDate = getEpochDay(timestamp);
		return getPageInterval(epochDate);
	}

	private @Nullable IPageInterval createPageInterval(BookInfoMetrics.CacheName cacheName, Long epochDay) {
		Instant start = toInstant(epochDay);
		Instant end = start.plus(1, ChronoUnit.DAYS).minus(1, ChronoUnit.NANOS);
		Collection<PageInfo> loaded = pagesLoader.load(id, start, end);
		return loaded.isEmpty() ? null : create(id, start, cacheName, loaded);
	}

	private static long currentEpochDay() {
		return System.currentTimeMillis() / MILLISECONDS_IN_DAY;
	}

	private static long getEpochDay(Instant instant) {
		return instant.getEpochSecond() / SECONDS_IN_DAY;
	}

	private static Instant toInstant(long epochDay) {
		return  Instant.ofEpochSecond(epochDay * SECONDS_IN_DAY);
	}

	private interface IPageInterval {
		BookInfoMetrics.CacheName getCacheName();
		int size();
		PageInfo get(PageId pageId);

		PageInfo find(Instant timestamp);

		PageInfo next(Instant startTimestamp);

		PageInfo previous(Instant startTimestamp);
	}

	private static final class EmptyPageInterval implements IPageInterval {
		@Override
		public BookInfoMetrics.CacheName getCacheName() {
			return null;
		}

		@Override
		public int size() {
			return 0;
		}

		@Override
		public PageInfo get(PageId pageId) {
			return null;
		}

		@Override
		public PageInfo find(Instant timestamp) {
			return null;
		}

		@Override
		public PageInfo next(Instant startTimestamp) {
			return null;
		}

		@Override
		public PageInfo previous(Instant startTimestamp) {
			return null;
		}
	}

	private static class PageInterval implements IPageInterval {
		private final BookInfoMetrics.CacheName cacheName;
		private final Map<PageId, PageInfo> pageById;
		private final NavigableMap<Instant, PageInfo> pageByInstant;

        private PageInterval(BookInfoMetrics.CacheName cacheName, Map<PageId, PageInfo> pageById, NavigableMap<Instant, PageInfo> pageByInstant) {
			this.cacheName = cacheName;
            this.pageById = pageById;
            this.pageByInstant = pageByInstant;
        }

		public BookInfoMetrics.CacheName getCacheName() {
			return cacheName;
		}

		@Override
		public int size() {
			return pageById.size();
		}

		@Override
		public PageInfo get(PageId pageId) {
			return pageById.get(pageId);
		}

		@Override
		public PageInfo find(Instant timestamp)
		{
			Entry<Instant, PageInfo> result = pageByInstant.floorEntry(timestamp);
			return result != null ? result.getValue() : null;
		}

		@Override
		public PageInfo next(Instant startTimestamp) {
			Entry<Instant, PageInfo> result = pageByInstant.higherEntry(startTimestamp);
			return result != null ? result.getValue() : null;
		}

		@Override
		public PageInfo previous(Instant startTimestamp) {
			Entry<Instant, PageInfo> result = pageByInstant.lowerEntry(startTimestamp);
			return result != null ? result.getValue() : null;
		}
	}

	private static PageInterval create(BookId id, Instant start, BookInfoMetrics.CacheName cacheName, Collection<PageInfo> pages) {
		Map<PageId, PageInfo> pageById = new HashMap<>();
		TreeMap<Instant, PageInfo> pageByInstant = new TreeMap<>();
		for (PageInfo page : pages) {
			//			if (page.getEnded() == null) {
//				throw new IllegalStateException(
//						"Page with '" + page.getId() + "' id hasn't got ended time"
//				);
//			}

			PageInfo previous = pageById.put(page.getId(), page);
			if (previous != null) {
				throw new IllegalStateException(
						"Page with '" + page.getId() + "' id is duplicated, previous: " +
								previous + ", current: " + page
				);
			}
			previous = pageByInstant.put(page.getId().getStart(), page);
			if (previous != null) {
				throw new IllegalStateException(
						"Page with '" + page.getId().getStart() + "' start time is duplicated, previous: " +
								previous + ", current: " + page
				);
			}
		}
		LOGGER.debug("Loaded {} pages for the book {}, {}", pages.size(), id, start);
		return new PageInterval(
				cacheName,
				unmodifiableMap(pageById),
				unmodifiableNavigableMap(pageByInstant)
		);
	}
}
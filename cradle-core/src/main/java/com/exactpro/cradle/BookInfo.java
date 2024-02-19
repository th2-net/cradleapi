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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Information about a book
 */
public class BookInfo
{
	private static final Logger LOGGER = LoggerFactory.getLogger(BookInfo.class);
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
				.build(this::createPageInterval);

		this.randomAccessCache = Caffeine.newBuilder()
				.maximumSize(cacheSize)
				.build(this::createPageInterval);
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
		return getPageInterval(pageId.getStart()).get(pageId);
	}
	
	public PageInfo findPage(Instant timestamp)
	{
		return getPageInterval(timestamp).find(timestamp);
	}
	
	public PageInfo getNextPage(Instant startTimestamp)
	{
		long epochDate = getEpochDay(startTimestamp);
		PageInfo currentInterval = getPageInterval(epochDate).next(startTimestamp);
		return currentInterval != null
				? currentInterval
				: getPageInterval(epochDate + 1).next(startTimestamp);
	}
	
	public PageInfo getPreviousPage(Instant startTimestamp)
	{
		long epochDate = getEpochDay(startTimestamp);
		PageInfo currentInterval = getPageInterval(epochDate).previous(startTimestamp);
		return currentInterval != null
				? currentInterval
				: getPageInterval(epochDate - 1).previous(startTimestamp);
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

	private @Nullable IPageInterval createPageInterval(Long epochDay) {
		Instant start = toInstant(epochDay);
		Instant end = start.plus(1, ChronoUnit.DAYS).minus(1, ChronoUnit.NANOS);
		Collection<PageInfo> loaded = pagesLoader.load(id, start, end);
		return loaded.isEmpty() ? null : create(loaded);
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

	@FunctionalInterface
	public interface PagesLoader {
		/**
		 * @return ordered page info collection from start to end date time. Both borders are included
		 */
		@Nonnull
		Collection<PageInfo> load(@Nonnull BookId bookId, @Nullable Instant start, @Nullable Instant end);
	}

	@FunctionalInterface
	public interface PageLoader {
		@Nullable
		PageInfo load(@Nonnull BookId bookId);
	}

	private interface IPageInterval {
		PageInfo get(PageId pageId);

		PageInfo find(Instant timestamp);

		PageInfo next(Instant startTimestamp);

		PageInfo previous(Instant startTimestamp);
	}

	private static final class EmptyPageInterval implements IPageInterval {
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
		private final Map<PageId, PageInfo> pageById = new HashMap<>();
		private final TreeMap<Instant, PageInfo> pageByInstant = new TreeMap<>();

        private PageInterval() { }

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

		private void add(PageInfo page) {
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

			LOGGER.debug("Added page {}", page);
		}
    }

	private static PageInterval create(Collection<PageInfo> pages) {
		PageInterval pageInterval = new PageInterval();
		for (PageInfo page : pages) {
			pageInterval.add(page);
		}
		return pageInterval;
	}
}
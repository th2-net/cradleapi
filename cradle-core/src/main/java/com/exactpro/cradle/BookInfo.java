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

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import com.exactpro.cradle.utils.CradleStorageException;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import org.apache.commons.lang3.concurrent.AtomicInitializer;
import org.apache.commons.lang3.concurrent.ConcurrentException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

/**
 * Information about a book
 */
public class BookInfo
{
	private static final Logger LOGGER = LoggerFactory.getLogger(BookInfo.class);
	private static final int SECONDS_IN_DAY = 60 * 60 * 24;
	private static final int MILLISECONDS_IN_DAY = SECONDS_IN_DAY * 1_000;
	private static final IPageInterval EMPTY_PAGE_INTERVAL = new EmptyPageInterval();
	private final BookId id;
	private final String fullName,
			desc;
	private final Instant created;

	private final LoadingCache<Long, IPageInterval> operateCache;
	private final LoadingCache<Long, IPageInterval> randomAccessCache;

	private final Map<PageId, PageInfo> pages;
	private final TreeMap<Instant, PageInfo> orderedPages;

	// AtomicInitializer call initialize method again if previous value is null
	private final AtomicInitializer<PageInfo> firstPage;
	private final PagesLoader pagesLoader;
	private final PageLoader lastPagesLoader;

	@SuppressWarnings("ResultOfMethodCallIgnored")
	public BookInfo(BookId id,
					String fullName,
					String desc,
					Instant created,
					PagesLoader pagesLoader,
					PageLoader firstPageLoader,
					PageLoader lastPageLoader) {
		this.id = id;
		this.fullName = fullName;
		this.desc = desc;
		this.created = created;
		this.pagesLoader = pagesLoader;
		this.lastPagesLoader = lastPageLoader;

		this.firstPage = new AtomicInitializer<>() {
            @Override
            protected PageInfo initialize() {
                return firstPageLoader.load(id);
            }
        };

		this.operateCache = Caffeine.newBuilder()
				.maximumSize(2)
				.build(this::createPageInterval);

		long currentEpochDay = currentEpochDay();
		this.operateCache.get(currentEpochDay - 1);
		this.operateCache.get(currentEpochDay);

		this.randomAccessCache = Caffeine.newBuilder()
				.maximumSize(10) // TODO: parametrised and weight option
				.build(this::createPageInterval);

		this.pages = null;
		this.orderedPages = null;
	}

	public BookInfo(BookId id, String fullName, String desc, Instant created, Collection<PageInfo> pages) throws CradleStorageException
	{
		this.id = id;
		this.fullName = fullName;
		this.desc = desc;
		this.created = created;
		this.pages = new ConcurrentHashMap<>();
		this.orderedPages = new TreeMap<>();
		this.pagesLoader = null;
		this.operateCache = null;
		this.randomAccessCache = null;
		this.firstPage = null;
		this.lastPagesLoader = null;

		if (pages == null)
			return;
		
		PageInfo notEndedPage = null;
		for (PageInfo p : pages)
		{
			this.pages.put(p.getId(), p);
			this.orderedPages.put(p.getStarted(), p);
			if (p.getEnded() == null)
			{
				if (notEndedPage != null)
					throw new CradleStorageException("Inconsistent state of book '"+id+"': "
							+ "page '"+ notEndedPage.getId().getName()+"' is not ended, "
							+ "but '"+p.getId().getName()+"' is not ended as well");
				notEndedPage = p;
			}
		}
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

	@Deprecated
	public Collection<PageInfo> getPages()
	{
		return Collections.unmodifiableCollection(orderedPages.values());
	}
	
	public @Nullable PageInfo getFirstPage()
	{
        try {
            return firstPage.get();
        } catch (ConcurrentException e) {
			LOGGER.error("Unexpected exception during first page lazy initialization", e);
            return null;
        }
    }
	
	public @Nullable PageInfo getLastPage()
	{
		return lastPagesLoader.load(id);
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

	@SuppressWarnings("ResultOfMethodCallIgnored")
	void invalidate() {
		operateCache.invalidateAll();
		randomAccessCache.invalidateAll();
		long currentEpochDate = currentEpochDay();
		operateCache.get(currentEpochDate - 1);
		operateCache.get(currentEpochDate);
	}
	
	void removePage(PageId pageId) {
		long epochDate = getEpochDay(pageId.getStart());
		long currentEpochDate = currentEpochDay();
		LoadingCache<Long, IPageInterval> cache = currentEpochDate - epochDate < 2
				? operateCache
				: randomAccessCache;

		IPageInterval pageInterval = cache.getIfPresent(epochDate);
		if (pageInterval != null) {
			pageInterval.remove(pageId);
		}
	}
	
	void addPage(PageInfo page) {
		long epochDate = getEpochDay(page.getId().getStart());
		long currentEpochDate = currentEpochDay();
		LoadingCache<Long, IPageInterval> cache = currentEpochDate - epochDate < 2
				? operateCache
				: randomAccessCache;

		cache.get(epochDate, key -> new PageInterval()).add(page);
	}

	@FunctionalInterface
	public interface PagesLoader {
		/**
		 * @return page info collection from start to end date time. Both borders are included
		 */
		Collection<PageInfo> load(BookId bookId, Instant start, Instant end);
	}

	private IPageInterval getPageInterval(long epochDate) {
		long currentEpochDate = currentEpochDay();
		IPageInterval pageInterval = currentEpochDate - epochDate < 2
				? operateCache.get(epochDate)
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
	public interface PageLoader {
		@Nullable
		PageInfo load(BookId bookId);
	}

	private interface IPageInterval {
		PageInfo get(PageId pageId);

		PageInfo find(Instant timestamp);

		PageInfo next(Instant startTimestamp);

		PageInfo previous(Instant startTimestamp);

		void remove(PageId pageId);

		void add(PageInfo page);
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

		@Override
		public void remove(PageId pageId) {
			throw new UnsupportedOperationException();
		}

		@Override
		public void add(PageInfo page) {
			throw new UnsupportedOperationException();
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
			Entry<Instant, PageInfo> result = pageByInstant.ceilingEntry(startTimestamp.plus(1, ChronoUnit.NANOS));
			return result != null ? result.getValue() : null;
		}

		@Override
		public PageInfo previous(Instant startTimestamp) {
			Entry<Instant, PageInfo> result = pageByInstant.floorEntry(startTimestamp.minus(1, ChronoUnit.NANOS));
			return result != null ? result.getValue() : null;
		}

		@Override
		public void remove(PageId pageId) {
			pageById.remove(pageId);
			pageByInstant.remove(pageId.getStart());
		}

		@Override
		public void add(PageInfo page) {
			PageInfo previous = pageById.put(page.getId(), page);
			if (previous != null) {
				throw new IllegalStateException(
						"Page with '" + page.getId() + "' ID is duplicated, previous: " +
								previous + ", current: " + page
				);
			}
			previous = pageByInstant.put(page.getStarted(), page);
			if (previous != null) {
				throw new IllegalStateException(
						"Page with '" + page.getStarted() + "' start time is duplicated, previous: " +
								previous + ", current: " + page
				);
			}

			LOGGER.debug("Added page {}", page);
		}
    }

	private static PageInterval create(Collection<PageInfo> pages) {
		PageInterval pageInterval = new PageInterval();
		for (PageInfo page : pages) {
			if (page.getRemoved() != null) {
				continue;
			}
			pageInterval.add(page);
		}
		return pageInterval;
	}
}
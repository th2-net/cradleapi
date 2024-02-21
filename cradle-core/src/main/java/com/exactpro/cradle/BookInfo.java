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
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static com.exactpro.cradle.BookInfoMetrics.CacheName.HOT;
import static com.exactpro.cradle.BookInfoMetrics.CacheName.RANDOM;
import static com.exactpro.cradle.BookInfoMetrics.RequestMethod.FIND;
import static com.exactpro.cradle.BookInfoMetrics.RequestMethod.GET;
import static com.exactpro.cradle.BookInfoMetrics.RequestMethod.NEXT;
import static com.exactpro.cradle.BookInfoMetrics.RequestMethod.PREVIOUS;
import static java.util.Collections.emptyIterator;
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
	private final Function<BookId, PageInfo> firstPageLoader;
	private final Function<BookId, PageInfo> lastPageLoader;

	public BookInfo(BookId id,
					String fullName,
					String desc,
					Instant created,
					int cacheSize,
					PagesLoader pagesLoader,
					Function<BookId, PageInfo> firstPageLoader,
					Function<BookId, PageInfo> lastPageLoader) {
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
			result = firstPageLoader.apply(id);
			if (!firstPage.compareAndSet(null, result)) {
				// another thread has initialized the reference
				result = firstPage.get();
			}
		}

		return result;
    }
	
	public @Nullable PageInfo getLastPage()
	{
		return lastPageLoader.apply(id);
	}
	
	public PageInfo getPage(PageId pageId)
	{
		IPageInterval pageInterval = getPageInterval(pageId.getStart());
		METRICS.incRequest(id.getName(), pageInterval.getCacheName(), GET);
		return pageInterval.get(pageId);
	}

	/**
	 * Requests page iterator using cache. Iterator lazy loads pages into cache.
	 */
	public Iterator<PageInfo> getPages(@Nullable Instant leftBoundTimestamp,
									   @Nullable Instant rightBoundTimestamp,
									   @Nonnull Order order) {
		Instant leftTimestamp = calculateBound(leftBoundTimestamp, this::getFirstPage);
		if (leftTimestamp == null) {
			return emptyIterator();
		}
		Instant rightTimestamp = calculateBound(rightBoundTimestamp, this::getLastPage);
		if (rightTimestamp == null) {
			return emptyIterator();
		}

		if (leftTimestamp.isAfter(rightTimestamp)) {
			throw new IllegalArgumentException(
					"Left bound '" + leftBoundTimestamp + " -> " + leftTimestamp +
							"' must be <= right bound '" + rightBoundTimestamp + " -> " + rightTimestamp + "'"
			);
		}
		long leftBoundEpochDay = getEpochDay(leftTimestamp);
		long rightBoundEpochDay = getEpochDay(rightTimestamp);

		Stream.Builder<Long> builder = Stream.builder();
		switch (order) {
            case DIRECT:
				for (long i = leftBoundEpochDay; i <= rightBoundEpochDay; i++) {
					builder.add(i);
				}
				break;
            case REVERSE:
				for (long i = rightBoundEpochDay; i >= leftBoundEpochDay; i--) {
					builder.add(i);
				}
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + order);
        }
		return builder.build()
				.flatMap(epochDay -> getPageInterval(epochDay).getPages(leftTimestamp, rightTimestamp, order))
				.iterator();
	}

	public PageInfo findPage(Instant timestamp) {
		// Search in the page interval related to the timestamp
		long epochDay = getEpochDay(timestamp);
		IPageInterval pageInterval = getPageInterval(epochDay);
		PageInfo pageInfo = pageInterval.find(timestamp);
		METRICS.incRequest(id.getName(), pageInterval.getCacheName(), FIND);
		if (pageInfo != null) {
			return pageInfo;
		}

		// Search in page intervals before the timestamp
		PageInfo firstPage = getFirstPage();
		if (firstPage == null) {
			return null;
		}
		if (firstPage.getStarted().isAfter(timestamp)) {
			return null;
		}

		long firstEpochDay = getEpochDay(firstPage.getStarted());
		if (epochDay < firstEpochDay) {
			return null;
		}
		if (epochDay > firstEpochDay) {
			for (long previousEpochDay = epochDay - 1; previousEpochDay >= firstEpochDay; previousEpochDay--) {
				IPageInterval previousPageInterval = getPageInterval(previousEpochDay);
				if (previousPageInterval.size() == 0) {
					// page interval without new pages
					continue;
				}
				PageInfo previousPageInfo = previousPageInterval.find(timestamp);
				METRICS.incRequest(id.getName(), previousPageInterval.getCacheName(), FIND);
				if (previousPageInfo == null) {
					throw new IllegalStateException(
							"Previous page interval isn't empty but doesn't contain page covers date " + timestamp
					);
				}
				return previousPageInfo;
			}
			throw new IllegalStateException(
					"First page is before requested timestamp, but neither cache contains appropriate page, requested: " +
							timestamp + ", first page: " + firstPage.getStarted()
			);
		}
		throw new IllegalStateException(
				"First page is in the same day as requested timestamp, but cache couldn't find appropriate page, requested: " +
						timestamp + ", first page: " + firstPage.getStarted()
		);
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

	// TODO: maybe change to refresh where cache is invalidated and reloaded
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

	private @Nullable Instant calculateBound(@Nullable Instant origin, Supplier<PageInfo> supplier) {
		if (origin != null) {
			PageInfo pageInfo = findPage(origin);
			if (pageInfo != null) {
				return pageInfo.getStarted();
			}
			return null;
		}
		PageInfo pageInfo = supplier.get();
		if (pageInfo == null) {
			return null;
		}
		return pageInfo.getStarted();
	}

	private void invalidate(long epochDay) {
		hotCache.invalidate(epochDay);
		randomAccessCache.invalidate(epochDay);
	}

	private IPageInterval getPageInterval(long epochDate) {
		long currentEpochDay = currentEpochDay();
		IPageInterval pageInterval = currentEpochDay - epochDate < 2
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

		Stream<PageInfo> getPages(Instant leftBoundTimestamp, Instant rightBoundTimestamp, Order order);
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

		@Override
		public Stream<PageInfo> getPages(Instant leftBoundTimestamp, Instant rightBoundTimestamp, Order order) {
			return Stream.empty();
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

		@Override
		public Stream<PageInfo> getPages(@Nonnull Instant leftBoundTimestamp,
										   @Nonnull Instant rightBoundTimestamp,
										   @Nonnull Order order) {
			Instant start = pageByInstant.floorKey(leftBoundTimestamp);
			NavigableMap<Instant, PageInfo> subMap = pageByInstant.subMap(
					start == null ? leftBoundTimestamp : start, true,
					rightBoundTimestamp, true
			);
			switch (order) {
                case DIRECT:
					return subMap.values().stream();
                case REVERSE:
					return subMap.descendingMap().values().stream();
				default:
					throw new IllegalArgumentException("Unsupported order: " + order);
            }
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
			previous = pageByInstant.put(page.getStarted(), page);
			if (previous != null) {
				throw new IllegalStateException(
						"Page with '" + page.getStarted() + "' start time is duplicated, previous: " +
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
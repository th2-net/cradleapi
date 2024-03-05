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
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.exactpro.cradle.BookInfoMetrics.CacheName.HOT;
import static com.exactpro.cradle.BookInfoMetrics.CacheName.RANDOM;
import static com.exactpro.cradle.BookInfoMetrics.RequestMethod.FIND;
import static com.exactpro.cradle.BookInfoMetrics.RequestMethod.GET;
import static com.exactpro.cradle.BookInfoMetrics.RequestMethod.ITERATE;
import static com.exactpro.cradle.BookInfoMetrics.RequestMethod.NEXT;
import static com.exactpro.cradle.BookInfoMetrics.RequestMethod.PREVIOUS;
import static com.exactpro.cradle.BookInfoMetrics.RequestMethod.REFRESH;
import static java.util.Collections.emptyIterator;
import static java.util.Collections.unmodifiableMap;
import static java.util.Collections.unmodifiableNavigableMap;

/**
 * Information about a book
 */
public class BookInfo {
    private static final Logger LOGGER = LoggerFactory.getLogger(BookInfo.class);
    private static final BookInfoMetrics METRICS = new BookInfoMetrics();
    private static final int HOT_CACHE_SIZE = 2;
    private static final int SECONDS_IN_DAY = 60 * 60 * 24;
    private static final int MILLISECONDS_IN_DAY = SECONDS_IN_DAY * 1_000;
    private static final long MAX_EPOCH_DAY = getEpochDay(Instant.MAX);
    private static final IPageInterval EMPTY_PAGE_INTERVAL = new EmptyPageInterval();

    private static final BookId EMPTY_BOOK_ID = new BookId("");

    static {
        METRICS.setPageCacheSize(EMPTY_BOOK_ID, HOT, HOT_CACHE_SIZE);
    }

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

    /**
     * This class provides access to pages related to the {@code id} book id.
     * Two cache ara used internally to hold pages relate to day interval
     * * Hot cache holds pages for the current and previous day. It can be refreshed by the {@link #refresh()} method
     * * Random access cache holds pages for days in the past. Book info invalidates this cache with the {@code raCacheInvalidateInterval} interval
     * @param raCacheSize - random access cache size. How many days of pages should be cached for the past.
     * @param raCacheInvalidateInterval - invalidate interval in millisecond for random access cache.
     */
    public BookInfo(BookId id,
                    String fullName,
                    String desc,
                    Instant created,
                    int raCacheSize,
                    long raCacheInvalidateInterval,
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
                .removalListener((epochDay, pageInterval, cause) -> METRICS.incInvalidate(id, HOT, cause))
                .build(epochDay -> createPageInterval(HOT, epochDay));

        this.randomAccessCache = Caffeine.newBuilder()
                .maximumSize(raCacheSize)
                .expireAfterWrite(raCacheInvalidateInterval, TimeUnit.MILLISECONDS)
                .removalListener((epochDay, pageInterval, cause) -> METRICS.incInvalidate(id, RANDOM, cause))
                .build(epochDay -> createPageInterval(RANDOM, epochDay));

        METRICS.setPageCacheSize(id, RANDOM, raCacheSize);
    }

    public BookId getId() {
        return id;
    }

    public String getFullName() {
        return fullName;
    }

    public String getDesc() {
        return desc;
    }

    public Instant getCreated() {
        return created;
    }

    /**
     * Requests pages from {@link PagesLoader} with book id and null start/end
     *
     * @return all ordered pages related to book id
     */
    public Collection<PageInfo> getPages() {
        return pagesLoader.load(id, null, null);
    }

    public @Nullable PageInfo getFirstPage() {
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

    public @Nullable PageInfo getLastPage() {
        return lastPageLoader.apply(id);
    }

    public PageInfo getPage(PageId pageId) {
        IPageInterval pageInterval = getPageInterval(pageId.getStart());
        return pageInterval.get(pageId);
    }

    /**
     * Requests page iterator using cache. Iterator lazy loads pages into cache when requested day isn't in there.
     *
     * @param leftBoundTimestamp  inclusive minimal timestamp. Start time of first page is used if passed value is null
     * @param rightBoundTimestamp inclusive maximum timestamp. Start time of last page is used if passed value is null
     */
    public Iterator<PageInfo> getPages(@Nullable Instant leftBoundTimestamp,
                                       @Nullable Instant rightBoundTimestamp,
                                       @Nonnull Order order) {
        if (leftBoundTimestamp != null &&
                rightBoundTimestamp != null &&
                leftBoundTimestamp.isAfter(rightBoundTimestamp)) {
            LOGGER.warn("Left bound '{}' should be <= right bound '{}'", leftBoundTimestamp, rightBoundTimestamp);
            return emptyIterator();
        }

        Instant leftTimestamp = calculateBound(leftBoundTimestamp, this::getFirstPage);
        if (leftTimestamp == null) {
            return emptyIterator();
        }
        Instant rightTimestamp = calculateBound(rightBoundTimestamp, this::getLastPage);
        if (rightTimestamp == null) {
            return emptyIterator();
        }

        if (leftTimestamp.isAfter(rightTimestamp)) {
            LOGGER.warn("Left calculated bound '{}' should be <= right calculated bound '{}'", leftTimestamp, rightTimestamp);
            return emptyIterator();
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
                .flatMap(epochDay -> getPageInterval(epochDay).stream(leftTimestamp, rightTimestamp, order))
                .iterator();
    }

    /**
     * Searches for a page that contains the specified timestamp.
     * Page contains the timestamp if it is inside the following interval `[start, end)` where:
     * - start - page start timestamp included
     * - end - page end timestamp excluded
     */
    public PageInfo findPage(Instant timestamp) {
        long epochDay = getEpochDay(timestamp);
        if (epochDay < 0) {
            return null;
        }
        if (epochDay >= MAX_EPOCH_DAY) {
            PageInfo lastPage = getLastPage();
            if (lastPage != null && lastPage.getEnded() == null) {
                return lastPage;
            }
            return null;
        }
        // Search in the page interval related to the timestamp
        IPageInterval pageInterval = getPageInterval(epochDay);
        PageInfo pageInfo = pageInterval.find(timestamp);
        if (pageInfo != null) {
            return pageInfo;
        }

        // Check first page
        PageInfo firstPage = getFirstPage();
        if (firstPage == null) {
            return null;
        }
        if (firstPage.getStarted().isAfter(timestamp)) {
            return null;
        }

        // Check last page
        PageInfo lastPage = getLastPage();
        if (lastPage == null) {
            return null;
        }
        if (lastPage.getEnded() != null && !timestamp.isBefore(lastPage.getEnded())) {
            return null;
        }

        // Search in page intervals before the timestamp
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
                // sP1 ... | ... eP1 ^ ... gap ... sP2 ... |
                return previousPageInterval.find(timestamp);
            }
            throw new IllegalStateException(
                    "First page is before requested timestamp, but neither cache contains appropriate page, requested: " +
                            timestamp + ", first page: " + firstPage.getStarted()
            );
        }
        // | ... sP1 ... eP1 ^ ... gap ... sP2 ... |
        // sP1 ... eP1 ^ ... | ... gap ... sP2 ... |
        return null;
    }

    public PageInfo getNextPage(Instant startTimestamp) {
        long epochDate = getEpochDay(startTimestamp);
        IPageInterval pageInterval = getPageInterval(epochDate);
        PageInfo currentInterval = pageInterval.next(startTimestamp);
        if (currentInterval != null) {
            return currentInterval;
        }
        pageInterval = getPageInterval(epochDate + 1);
        return pageInterval.next(startTimestamp);
    }

    public PageInfo getPreviousPage(Instant startTimestamp) {
        long epochDate = getEpochDay(startTimestamp);
        IPageInterval pageInterval = getPageInterval(epochDate);
        PageInfo currentInterval = pageInterval.previous(startTimestamp);
        if (currentInterval != null) {
            return currentInterval;
        }
        pageInterval = getPageInterval(epochDate - 1);
        return pageInterval.previous(startTimestamp);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BookInfo bookInfo = (BookInfo) o;
        return Objects.equals(id, bookInfo.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public String toString() {
        return "BookInfo{" +
                "id=" + id +
                ", fullName='" + fullName + '\'' +
                ", desc='" + desc + '\'' +
                ", created=" + created +
                '}';
    }

    /**
     * Refreshes: first page and hot cache
     * Invalidates: random access cache
     */
    void refresh() {
        firstPage.set(null);
        METRICS.incRequest(id, HOT, REFRESH);
        hotCache.invalidateAll();

        getFirstPage();
        long currentEpochDay = currentEpochDay();
        for (int shift = HOT_CACHE_SIZE - 1; shift >= 0; shift--) {
            this.hotCache.get(currentEpochDay - shift);
        }
    }

    void invalidate(Instant timestamp) {
        long epochDay = getEpochDay(timestamp);
        invalidate(epochDay);
    }

    void invalidate(Iterable<Instant> timestamps) {
        StreamSupport.stream(timestamps.spliterator(), false)
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
            return origin;
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
        long diff = currentEpochDay - epochDate;
        LoadingCache<Long, IPageInterval> cache = 0 <= diff && diff < 2 ? hotCache : randomAccessCache;
        IPageInterval pageInterval = cache.get(epochDate);
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
        if (loaded.isEmpty()) {
            return null;
        }
        // We shouldn't register `load` when day page interval (interval in the next) is empty because don't register `request` for empty interval.
        // `EmptyPageInterval` handles `requests` for empty interval and doesn't register `request`
        // `loads` and `requests` are required for calculating hit / miss rate
        METRICS.incLoads(id, cacheName, loaded.size());
        return create(id, start, cacheName, loaded);
    }

    private static long currentEpochDay() {
        return System.currentTimeMillis() / MILLISECONDS_IN_DAY;
    }

    private static long getEpochDay(Instant instant) {
        return instant.getEpochSecond() / SECONDS_IN_DAY;
    }

    private static Instant toInstant(long epochDay) {
        return Instant.ofEpochSecond(epochDay * SECONDS_IN_DAY);
    }

    private interface IPageInterval {
        int size();

        PageInfo get(PageId pageId);

        PageInfo find(Instant timestamp);

        PageInfo next(Instant startTimestamp);

        PageInfo previous(Instant startTimestamp);

        Stream<PageInfo> stream(Instant leftBoundTimestamp, Instant rightBoundTimestamp, Order order);
    }

    private static final class EmptyPageInterval implements IPageInterval {

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
        public Stream<PageInfo> stream(Instant leftBoundTimestamp, Instant rightBoundTimestamp, Order order) {
            return Stream.empty();
        }
    }

    private static class PageInterval implements IPageInterval {
        private final BookId bookId;
        private final BookInfoMetrics.CacheName cacheName;
        private final Map<PageId, PageInfo> pageById;
        private final NavigableMap<Instant, PageInfo> pageByInstant;

        private PageInterval(BookId bookId, BookInfoMetrics.CacheName cacheName, Map<PageId, PageInfo> pageById, NavigableMap<Instant, PageInfo> pageByInstant) {
            this.bookId = bookId;
            this.cacheName = cacheName;
            this.pageById = pageById;
            this.pageByInstant = pageByInstant;
        }

        @Override
        public int size() {
            return pageById.size();
        }

        @Override
        public PageInfo get(PageId pageId) {
            METRICS.incRequest(bookId, cacheName, GET);
            return pageById.get(pageId);
        }

        @Override
        public PageInfo find(Instant timestamp) {
            METRICS.incRequest(bookId, cacheName, FIND);
            Entry<Instant, PageInfo> result = pageByInstant.floorEntry(timestamp);
            if (result == null) {
                return null;
            }
            PageInfo pageInfo = result.getValue();
            if (pageInfo.getEnded() == null) {
                return pageInfo;
            }
            if (!timestamp.isBefore(pageInfo.getEnded())) { // the page's end timestamp is excluded from the page's interval
                return null;
            }
            return pageInfo;
        }

        @Override
        public PageInfo next(Instant startTimestamp) {
            METRICS.incRequest(bookId, cacheName, NEXT);
            Entry<Instant, PageInfo> result = pageByInstant.higherEntry(startTimestamp);
            return result != null ? result.getValue() : null;
        }

        @Override
        public PageInfo previous(Instant startTimestamp) {
            METRICS.incRequest(bookId, cacheName, PREVIOUS);
            Entry<Instant, PageInfo> result = pageByInstant.lowerEntry(startTimestamp);
            return result != null ? result.getValue() : null;
        }

        @Override
        public Stream<PageInfo> stream(@Nonnull Instant leftBoundTimestamp,
                                       @Nonnull Instant rightBoundTimestamp,
                                       @Nonnull Order order) {
            METRICS.incRequest(bookId, cacheName, ITERATE);
            Instant start = pageByInstant.floorKey(leftBoundTimestamp);
            NavigableMap<Instant, PageInfo> subMap = pageByInstant.subMap(
                    start == null ? leftBoundTimestamp : start, true,
                    rightBoundTimestamp, true
            );

            Predicate<PageInfo> predicate = pageInfo -> !(pageInfo.getStarted().isAfter(rightBoundTimestamp) ||
                    (pageInfo.getEnded() != null && !leftBoundTimestamp.isBefore(pageInfo.getEnded())));

            switch (order) {
                case DIRECT:
                    return subMap.values().stream().filter(predicate);
                case REVERSE:
                    return subMap.descendingMap().values().stream().filter(predicate);
                default:
                    throw new IllegalArgumentException("Unsupported order: " + order);
            }
        }
    }

    private static PageInterval create(BookId id, Instant start, BookInfoMetrics.CacheName cacheName, Collection<PageInfo> pages) {
        Map<PageId, PageInfo> pageById = new HashMap<>();
        TreeMap<Instant, PageInfo> pageByInstant = new TreeMap<>();
        for (PageInfo page : pages) {
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
                id, cacheName,
                unmodifiableMap(pageById),
                unmodifiableNavigableMap(pageByInstant)
        );
    }
}
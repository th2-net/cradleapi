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

import com.exactpro.cradle.BookInfoMetrics.CacheName;
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
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.exactpro.cradle.BookInfoMetrics.CacheName.HOT;
import static com.exactpro.cradle.BookInfoMetrics.CacheName.RANDOM;
import static com.exactpro.cradle.BookInfoMetrics.RequestMethod.FIND;
import static com.exactpro.cradle.BookInfoMetrics.RequestMethod.GET;
import static com.exactpro.cradle.BookInfoMetrics.RequestMethod.ITERATE;
import static com.exactpro.cradle.BookInfoMetrics.RequestMethod.NEXT;
import static com.exactpro.cradle.BookInfoMetrics.RequestMethod.PREVIOUS;
import static com.exactpro.cradle.BookInfoMetrics.RequestMethod.REFRESH;
import static com.exactpro.cradle.BookInfoMetrics.incInvalidate;
import static com.exactpro.cradle.BookInfoMetrics.incLoad;
import static com.exactpro.cradle.BookInfoMetrics.incRequest;
import static com.exactpro.cradle.BookInfoMetrics.setPageCacheSize;
import static java.util.Collections.emptyIterator;
import static java.util.Collections.unmodifiableMap;
import static java.util.Collections.unmodifiableNavigableMap;

/**
 * Information about a book
 */
public class BookInfo {
    private static final Logger LOGGER = LoggerFactory.getLogger(BookInfo.class);
    private static final int HOT_CACHE_SIZE = 2;
    private static final int SECONDS_IN_DAY = 60 * 60 * 24;
    private static final int MILLISECONDS_IN_DAY = SECONDS_IN_DAY * 1_000;
    private static final long MAX_EPOCH_DAY = getEpochDay(Instant.MAX);
    private static final IPageInterval EMPTY_PAGE_INTERVAL = new EmptyPageInterval();

    private static final BookId EMPTY_BOOK_ID = new BookId("");

    static {
        setPageCacheSize(EMPTY_BOOK_ID, HOT, HOT_CACHE_SIZE);
    }

    private final BookId id;
    private final String fullName,
            desc;
    private final Instant created;

    private final LoadingCache<Long, IPageInterval> hotCache;
    private final LoadingCache<Long, IPageInterval> randomAccessCache;

    // AtomicInitializer call initialize method again if previous value is null
    private final AtomicReference<PageInfo> firstPage = new AtomicReference<>();
    // AtomicInitializer call initialize method again if previous value is null
    private final AtomicReference<PageInfo> lastPage = new AtomicReference<>();
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
                .removalListener((epochDay, pageInterval, cause) -> incInvalidate(id, HOT, cause))
                .build(epochDay -> createPageInterval(HOT, epochDay));

        this.randomAccessCache = Caffeine.newBuilder()
                .maximumSize(raCacheSize)
                .expireAfterWrite(raCacheInvalidateInterval, TimeUnit.MILLISECONDS)
                .removalListener((epochDay, pageInterval, cause) -> incInvalidate(id, RANDOM, cause))
                .build(epochDay -> createPageInterval(RANDOM, epochDay));

        setPageCacheSize(id, RANDOM, raCacheSize);
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
        return getPageInfo(firstPage, firstPageLoader);
    }

    public @Nullable PageInfo getLastPage() {
        return getPageInfo(lastPage, lastPageLoader);
    }

    public @Nullable PageInfo getPage(PageId pageId) {
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

        if (getFirstPage() == null) {
            return emptyIterator();
        }

        if (rightBoundTimestamp != null && getFirstPage().getStarted().isAfter(rightBoundTimestamp)) {
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
                .distinct()
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
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("find page for {} epoch {}, current cache hot: {}, random: {}",
                    timestamp, epochDay, hotCache.asMap().keySet(), randomAccessCache.asMap().keySet());
        } else if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("find page for {} epoch {}", timestamp, epochDay);
        }

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
        return pageInterval.find(timestamp);
    }

    public PageInfo getNextPage(Instant startTimestamp) {
        long epochDate = getEpochDay(startTimestamp);
        IPageInterval pageInterval = getPageInterval(epochDate);
        PageInfo currentInterval = pageInterval.next(startTimestamp);
        if (currentInterval != null) {
            return currentInterval;
        }

        PageInfo lastPage = getLastPage();
        if (lastPage == null) {
            return null;
        }

        for (long i = epochDate + 1; i <= getEpochDay(lastPage.getStarted()); i++) {
            pageInterval = getPageInterval(i);
            PageInfo pageInfo = pageInterval.next(startTimestamp);
            if (pageInfo != null) {
                return pageInfo;
            }
        }
        return null;
    }

    public PageInfo getPreviousPage(Instant startTimestamp) {
        long epochDate = getEpochDay(startTimestamp);
        IPageInterval pageInterval = getPageInterval(epochDate);
        PageInfo currentInterval = pageInterval.previous(startTimestamp);
        if (currentInterval != null) {
            return currentInterval;
        }

        PageInfo firstPage = getFirstPage();
        if (firstPage == null) {
            return null;
        }

        for (long i = epochDate - 1; getEpochDay(firstPage.getStarted()) <= i; i--) {
            pageInterval = getPageInterval(i);
            PageInfo pageInfo = pageInterval.previous(startTimestamp);
            if (pageInfo != null) {
                return pageInfo;
            }
        }
        return null;
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
    public void refresh() {
        firstPage.set(null);
        lastPage.set(null);
        incRequest(id, HOT, REFRESH);
        hotCache.invalidateAll();
        randomAccessCache.invalidateAll();

        getFirstPage();
        getLastPage();
        long currentEpochDay = currentEpochDay();
        for (int shift = HOT_CACHE_SIZE - 1; shift >= 0; shift--) {
            this.hotCache.get(currentEpochDay - shift);
        }
    }

    void invalidate(Instant timestamp) {
        long epochDay = getEpochDay(timestamp);
        invalidateFistPage(epochDay);
        invalidateLastPage(epochDay);
        invalidate(epochDay);
    }

    void invalidate(Iterable<Instant> timestamps) {
        timestamps.forEach(this::invalidate);
    }

    private void invalidateFistPage(long epochDay) {
        PageInfo current = firstPage.get();
        if (current != null
                && (epochDay <= getEpochDay(current.getStarted()) || current.getEnded() == null || getEpochDay(current.getEnded()) <= epochDay)
        ) {
            firstPage.compareAndSet(current, null);
        }
    }

    private void invalidateLastPage(long epochDay) {
        PageInfo current = lastPage.get();
        if (current != null && getEpochDay(current.getStarted()) <= epochDay) {
            lastPage.compareAndSet(current, null);
        }
    }

    private PageInfo getPageInfo(AtomicReference<PageInfo> refToPage, Function<BookId, PageInfo> pageLoader) {
        PageInfo result = refToPage.get();

        if (result == null) {
            result = pageLoader.apply(id);
            if (!refToPage.compareAndSet(null, result)) {
                // another thread has initialized the reference
                result = refToPage.get();
            }
        }

        return result;
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
        LoadingCache<Long, IPageInterval> cache = 0 <= diff && diff < HOT_CACHE_SIZE ? hotCache : randomAccessCache;
        LOGGER.debug("Get page interval for epoch date {}", epochDate);
        IPageInterval pageInterval = cache.get(epochDate);
        return pageInterval != null
                ? pageInterval
                : EMPTY_PAGE_INTERVAL;
    }

    private IPageInterval getPageInterval(Instant timestamp) {
        long epochDate = getEpochDay(timestamp);
        return getPageInterval(epochDate);
    }

    private @Nullable IPageInterval createPageInterval(CacheName cacheName, Long epochDay) {
        Instant start = toInstant(epochDay);
        Instant end = start.plus(1, ChronoUnit.DAYS).minus(1, ChronoUnit.NANOS);
        Collection<PageInfo> pages = pagesLoader.load(id, start, end);
        LOGGER.debug("Loaded pages id: {}, epoch: {} [{} - {}], pages: {}",
                id, epochDay, start, end, pages.size());
        incLoad(id, cacheName, pages.size());
        if (pages.isEmpty()) {
            return null;
        }
        Collection<PageInfo> filteredPages = pages.stream().filter(pageInfo -> {
            Instant pageStart = pageInfo.getStarted();
            Instant pageEnd = pageInfo.getEnded() == null ? Instant.MAX : pageInfo.getEnded();
            return pageStart.isBefore(end) && !start.isAfter(pageEnd);
        }).collect(Collectors.toList());
        if (filteredPages.size() != pages.size()) {
            LOGGER.warn("Pages has been filtered after loading id: {}, epoch: {} [{} - {}], size: {} -> {}",
                    id, epochDay, start, end, pages.size(), filteredPages.size());
        }
        return create(id, cacheName, filteredPages);
    }

    static long currentEpochDay() {
        return System.currentTimeMillis() / MILLISECONDS_IN_DAY;
    }

    static long getEpochDay(Instant instant) {
        return instant.getEpochSecond() / SECONDS_IN_DAY;
    }

    private static Instant toInstant(long epochDay) {
        return Instant.ofEpochSecond(epochDay * SECONDS_IN_DAY);
    }

    private interface IPageInterval {

        PageInfo get(PageId pageId);

        PageInfo find(Instant timestamp);

        PageInfo next(Instant startTimestamp);

        PageInfo previous(Instant startTimestamp);

        Stream<PageInfo> stream(Instant leftBoundTimestamp, Instant rightBoundTimestamp, Order order);
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
        public Stream<PageInfo> stream(Instant leftBoundTimestamp, Instant rightBoundTimestamp, Order order) {
            return Stream.empty();
        }
    }

    private static class PageInterval implements IPageInterval {
        private final BookId bookId;
        private final CacheName cacheName;
        private final Map<PageId, PageInfo> pageById;
        private final NavigableMap<Instant, PageInfo> pageByInstant;

        private PageInterval(BookId bookId, CacheName cacheName, Map<PageId, PageInfo> pageById, NavigableMap<Instant, PageInfo> pageByInstant) {
            this.bookId = bookId;
            this.cacheName = cacheName;
            this.pageById = pageById;
            this.pageByInstant = pageByInstant;
        }

        @Override
        public PageInfo get(PageId pageId) {
            incRequest(bookId, cacheName, GET);
            return pageById.get(pageId);
        }

        @Override
        public PageInfo find(Instant timestamp) {
            incRequest(bookId, cacheName, FIND);
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
            incRequest(bookId, cacheName, NEXT);
            Entry<Instant, PageInfo> result = pageByInstant.higherEntry(startTimestamp);
            return result != null ? result.getValue() : null;
        }

        @Override
        public PageInfo previous(Instant startTimestamp) {
            incRequest(bookId, cacheName, PREVIOUS);
            Entry<Instant, PageInfo> firstSearch = pageByInstant.lowerEntry(startTimestamp);
            if (firstSearch == null) {
                return null;
            }
            PageInfo pageInfo = firstSearch.getValue();
            if (pageInfo.getEnded() == null
                    || pageInfo.getEnded().isAfter(startTimestamp)) {
                Entry<Instant, PageInfo> secondSearch = pageByInstant.lowerEntry(pageInfo.getStarted());
                return secondSearch != null ? secondSearch.getValue() : null;
            }
            return pageInfo;
        }

        @Override
        public Stream<PageInfo> stream(@Nonnull Instant leftBoundTimestamp,
                                       @Nonnull Instant rightBoundTimestamp,
                                       @Nonnull Order order) {
            incRequest(bookId, cacheName, ITERATE);
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

    private static PageInterval create(BookId id, CacheName cacheName, Collection<PageInfo> pages) {
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
        return new PageInterval(
                id, cacheName,
                unmodifiableMap(pageById),
                unmodifiableNavigableMap(pageByInstant)
        );
    }
}
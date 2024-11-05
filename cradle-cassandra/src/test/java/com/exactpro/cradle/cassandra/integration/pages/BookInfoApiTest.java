/*
 * Copyright 2024 Exactpro (Exactpro Systems Limited)
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
package com.exactpro.cradle.cassandra.integration.pages;

import com.exactpro.cradle.BookId;
import com.exactpro.cradle.BookInfo;
import com.exactpro.cradle.BookToAdd;
import com.exactpro.cradle.Order;
import com.exactpro.cradle.PageId;
import com.exactpro.cradle.PageInfo;
import com.exactpro.cradle.PageToAdd;
import com.exactpro.cradle.cassandra.CassandraCradleStorage;
import com.exactpro.cradle.cassandra.integration.CassandraCradleHelper;
import com.exactpro.cradle.utils.CradleStorageException;
import com.google.common.collect.Iterators;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.exactpro.cradle.BookInfoMetrics.CacheName.HOT;
import static com.exactpro.cradle.BookInfoMetrics.CacheName.RANDOM;
import static com.exactpro.cradle.BookInfoMetrics.getLoadCount;
import static com.exactpro.cradle.Order.DIRECT;
import static java.time.temporal.ChronoUnit.DAYS;
import static java.time.temporal.ChronoUnit.HOURS;
import static java.time.temporal.ChronoUnit.NANOS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

public class BookInfoApiTest {
    private static final Instant NOW = Instant.now();
    private static final BookId BOOK_ID = new BookId("test_book");

    private static final PageInfo PAGE_0 = new PageInfo( // -2 00:00 - -1 00:00
            new PageId(BOOK_ID, NOW.minus(2, DAYS).truncatedTo(DAYS), "page-0"),
            NOW.minus(1, DAYS).truncatedTo(DAYS), "");
    private static final PageInfo PAGE_1 = new PageInfo( // -1 00:00 - 1 00:00
            new PageId(BOOK_ID, NOW.minus(1, DAYS).truncatedTo(DAYS), "page-1"),
            NOW.plus(1, DAYS).truncatedTo(DAYS), "");
    private static final PageInfo PAGE_2 = new PageInfo( // 1 00:00 - 1 12:00
            new PageId(BOOK_ID, NOW.plus(1, DAYS).truncatedTo(DAYS), "page-2"),
            NOW.plus(1, DAYS).truncatedTo(DAYS).plus(12, HOURS), "");
    private static final PageInfo PAGE_3 = new PageInfo( // 1 12:00 - 2 12:00
            new PageId(BOOK_ID, NOW.plus(1, DAYS).truncatedTo(DAYS).plus(12, HOURS), "page-3"),
            NOW.plus(2, DAYS).truncatedTo(DAYS).plus(12, HOURS), "");
    private static final PageInfo PAGE_4 = new PageInfo( // 2 12:00 - max
            new PageId(BOOK_ID, NOW.plus(2, DAYS).truncatedTo(DAYS).plus(12, HOURS), "page-4"),
            null, "");
    private static final List<PageInfo> PAGES = List.of(PAGE_1, PAGE_2, PAGE_3, PAGE_4);

    private CassandraCradleStorage storage;
    private BookInfo bookInfo;
    private double hotCacheLoad;
    private double randomCacheLoad;

    @BeforeClass
    public void beforeClass() throws IOException, CradleStorageException {
        storage = CassandraCradleHelper.getInstance().getStorage();
        storage.addBook(new BookToAdd(BOOK_ID.getName(), NOW.minus(100, DAYS)));
        storage.addPages(BOOK_ID, Stream.concat(Stream.of(PAGE_0), PAGES.stream())
                .map(pageInfo -> new PageToAdd(pageInfo.getName(), pageInfo.getStarted(), pageInfo.getComment()))
                .collect(Collectors.toList()));
        storage.removePage(PAGE_0.getId());
    }

    @BeforeMethod
    public void beforeMethod() throws CradleStorageException {
        bookInfo = storage.getBook(BOOK_ID);

        // refresh is done after 1 min after test start by default
        hotCacheLoad = getLoadCount(BOOK_ID, HOT);
        randomCacheLoad = getLoadCount(BOOK_ID, RANDOM);

        bookInfo.refresh();
        assertLoadCount(hotCacheLoad += 2, randomCacheLoad, "after book refresh");
    }

    @Test(description = "Find page in page cache")
    public void findPageTest() {
        assertLoadCount(hotCacheLoad, randomCacheLoad, "before -2 days request");
        assertNull(bookInfo.findPage(NOW.minus(2, DAYS).truncatedTo(DAYS)));
        assertLoadCount(hotCacheLoad, randomCacheLoad += 1, "after -2 days request");

        assertLoadCount(hotCacheLoad, randomCacheLoad, "before -1 day request");
        assertEquals(bookInfo.findPage(NOW.minus(1, DAYS).truncatedTo(DAYS)).getName(), PAGE_1.getName());
        assertLoadCount(hotCacheLoad, randomCacheLoad, "after -1 day request");

        assertLoadCount(hotCacheLoad, randomCacheLoad, "before today request");
        assertEquals(bookInfo.findPage(NOW.truncatedTo(DAYS)).getName(), PAGE_1.getName());
        assertLoadCount(hotCacheLoad, randomCacheLoad, "after today request");

        assertLoadCount(hotCacheLoad, randomCacheLoad, "before +1 day request");
        assertEquals(bookInfo.findPage(NOW.plus(1, DAYS).truncatedTo(DAYS)).getName(), PAGE_2.getName());
        assertLoadCount(hotCacheLoad, randomCacheLoad += 1, "after +1 day request");

        assertLoadCount(hotCacheLoad, randomCacheLoad, "before +2 days request");
        assertEquals(bookInfo.findPage(NOW.plus(2, DAYS).truncatedTo(DAYS)).getName(), PAGE_3.getName());
        assertLoadCount(hotCacheLoad, randomCacheLoad += 1, "after +2 days request");

        assertLoadCount(hotCacheLoad, randomCacheLoad, "before +3 days request");
        assertEquals(bookInfo.findPage(NOW.plus(3, DAYS).truncatedTo(DAYS)).getName(), PAGE_4.getName());
        assertLoadCount(hotCacheLoad, randomCacheLoad += 1, "after +3 days request");

        assertLoadCount(hotCacheLoad, randomCacheLoad, "after all checked requests");

        // repeat request which return not null result
        for (int i = -1; i <= 3; i++) {
            bookInfo.findPage(NOW.plus(i, DAYS).truncatedTo(DAYS));
            assertLoadCount(hotCacheLoad, randomCacheLoad, "after " + i + " day(s) in repeated request");
        }
    }

    @Test(description = "Get pages")
    public void getPagesTest() {
        assertEquals(
                bookInfo.getPages().stream().map(PageInfo::getName).collect(Collectors.toList()),
                PAGES.stream().map(PageInfo::getName).collect(Collectors.toList())
        );

        assertLoadCount(hotCacheLoad, randomCacheLoad, "after get pages");
    }

    @Test(description = "Get pages by time in direct order", dataProvider = "orders")
    public void getPagesByTimeTest(Order order) {
        assertLoadCount(hotCacheLoad, randomCacheLoad, "before get pages before first page");
        assertFalse(bookInfo.getPages(NOW.minus(4, DAYS), NOW.minus(3, DAYS), order).hasNext());
        assertLoadCount(hotCacheLoad, randomCacheLoad, "after get pages before first page");

        assertLoadCount(hotCacheLoad, randomCacheLoad, "before get [1, 2] pages");
        assertEquals(
                extractNames(bookInfo.getPages(
                        NOW.minus(1, DAYS).truncatedTo(DAYS),
                        NOW.plus(1, DAYS).truncatedTo(DAYS)
                                .plus(12, HOURS).minus(1, NANOS),
                        order)),
                extractNames(Iterators.forArray(PAGE_1, PAGE_2), order)
        );
        assertLoadCount(hotCacheLoad, randomCacheLoad += 1, "after get [1, 2] pages");

        assertLoadCount(hotCacheLoad, randomCacheLoad, "before get [3, 4] pages");
        assertEquals(
                extractNames(bookInfo.getPages(
                        NOW.plus(1, DAYS).truncatedTo(DAYS)
                                .plus(12, HOURS),
                        NOW.plus(3, DAYS).truncatedTo(DAYS)
                                .minus(1, NANOS),
                        order)),
                extractNames(Iterators.forArray(PAGE_3, PAGE_4), order)
        );
        assertLoadCount(hotCacheLoad, randomCacheLoad += 1, "after get [3, 4] pages");

        assertLoadCount(hotCacheLoad, randomCacheLoad, "before get pages after last page");
        assertEquals(
                extractNames(bookInfo.getPages(
                        NOW.plus(3, DAYS),
                        NOW.plus(4, DAYS),
                        order)),
                extractNames(Iterators.forArray(PAGE_4))
                );
        assertLoadCount(hotCacheLoad, randomCacheLoad += 2, "after get pages after last page");

        assertLoadCount(hotCacheLoad, randomCacheLoad, "before get all pages");
        assertEquals(
                extractNames(bookInfo.getPages(
                        NOW.minus(1, DAYS).truncatedTo(DAYS),
                        NOW.plus(3, DAYS).truncatedTo(DAYS)
                                .minus(1, NANOS),
                        order)),
                extractNames(PAGES.iterator(), order)
        );
        assertLoadCount(hotCacheLoad, randomCacheLoad, "after get all pages");
    }

    @Test(description = "Get first page")
    public void getFirstPageTest() {
        assertLoadCount(hotCacheLoad, randomCacheLoad, "before first page request");
        PageInfo firstPage = bookInfo.getFirstPage();
        assertNotNull(firstPage);
        assertEquals(firstPage.getName(), PAGE_1.getName());
        assertLoadCount(hotCacheLoad, randomCacheLoad, "after first page request");
    }

    @Test(description = "Get last page")
    public void getLastPageTest() {
        assertLoadCount(hotCacheLoad, randomCacheLoad, "before first page request");
        PageInfo lastPage = bookInfo.getLastPage();
        assertNotNull(lastPage);
        assertEquals(lastPage.getName(), PAGE_4.getName());
        assertLoadCount(hotCacheLoad, randomCacheLoad, "after first page request");
    }

    @Test(description = "Get page")
    public void getPageTest() {
        assertLoadCount(hotCacheLoad, randomCacheLoad, "before page 1 request");
        PageInfo page1 = bookInfo.getPage(PAGE_1.getId());
        assertNotNull(page1);
        assertEquals(page1.getName(), PAGE_1.getName());
        assertLoadCount(hotCacheLoad, randomCacheLoad, "after page 1 request");

        assertLoadCount(hotCacheLoad, randomCacheLoad, "before page 2 request");
        PageInfo page2 = bookInfo.getPage(PAGE_2.getId());
        assertNotNull(page2);
        assertEquals(page2.getName(), PAGE_2.getName());
        assertLoadCount(hotCacheLoad, randomCacheLoad += 1, "after page 2 request");

        assertLoadCount(hotCacheLoad, randomCacheLoad, "before page 3 request");
        PageInfo page3 = bookInfo.getPage(PAGE_3.getId());
        assertNotNull(page3);
        assertEquals(page3.getName(), PAGE_3.getName());
        assertLoadCount(hotCacheLoad, randomCacheLoad, "after page 3 request");

        assertLoadCount(hotCacheLoad, randomCacheLoad, "before page 4 request");
        PageInfo page4 = bookInfo.getPage(PAGE_4.getId());
        assertNotNull(page4);
        assertEquals(page4.getName(), PAGE_4.getName());
        assertLoadCount(hotCacheLoad, randomCacheLoad += 1, "after page 4 request");
    }

    @Test(description = "Get next page")
    public void getNextPageTest() {
        assertLoadCount(hotCacheLoad, randomCacheLoad, "before next page (page1.start - 1 NANO) request");
        PageInfo page1 = bookInfo.getNextPage(PAGE_1.getStarted().minus(1, NANOS));
        assertNotNull(page1);
        assertEquals(page1.getName(), PAGE_1.getName());
        // loaded -2 day
        assertLoadCount(hotCacheLoad, randomCacheLoad += 1, "after next page (page1.start - 1 NANO) request");

        assertLoadCount(hotCacheLoad, randomCacheLoad, "before next page (page2.start - 1 NANO) request");
        PageInfo page2 = bookInfo.getNextPage(PAGE_2.getStarted().minus(1, NANOS));
        assertNotNull(page2);
        assertEquals(page2.getName(), PAGE_2.getName());
        // loaded +1 day
        assertLoadCount(hotCacheLoad, randomCacheLoad += 1, "after next page (page2.start - 1 NANO) request");

        assertLoadCount(hotCacheLoad, randomCacheLoad, "before next page (page3.start - 1 NANO) request");
        PageInfo page3 = bookInfo.getNextPage(PAGE_3.getStarted().minus(1, NANOS));
        assertNotNull(page3);
        assertEquals(page3.getName(), PAGE_3.getName());
        assertLoadCount(hotCacheLoad, randomCacheLoad, "after next page (page3.start - 1 NANO) request");

        assertLoadCount(hotCacheLoad, randomCacheLoad, "before next page (page4.start - 1 NANO) request");
        PageInfo page4 = bookInfo.getNextPage(PAGE_4.getStarted().minus(1, NANOS));
        assertNotNull(page4);
        assertEquals(page4.getName(), PAGE_4.getName());
        // loaded +2 day
        assertLoadCount(hotCacheLoad, randomCacheLoad += 1, "after next page (page4.start - 1 NANO) request");

        assertLoadCount(hotCacheLoad, randomCacheLoad, "before next page (page4.start) request");
        assertNull(bookInfo.getNextPage(PAGE_4.getStarted()));
        assertLoadCount(hotCacheLoad, randomCacheLoad, "after next page (page4.start) request");
    }

    @Test(description = "Get previous page")
    public void getPreviousPageTest() {
        assertLoadCount(hotCacheLoad, randomCacheLoad, "before previous page (page4.start) request");
        PageInfo page3 = bookInfo.getPreviousPage(PAGE_4.getStarted());
        assertNotNull(page3);
        assertEquals(page3.getName(), PAGE_3.getName());
        // loaded +2 day
        assertLoadCount(hotCacheLoad, randomCacheLoad += 1, "after previous page (page4.start) request");

        assertLoadCount(hotCacheLoad, randomCacheLoad, "before previous page (page3.start) request");
        PageInfo page2 = bookInfo.getPreviousPage(PAGE_3.getStarted());
        assertNotNull(page2);
        assertEquals(page2.getName(), PAGE_2.getName());
        // loaded +1 day
        assertLoadCount(hotCacheLoad, randomCacheLoad += 1, "after previous page (page3.start) request");

        assertLoadCount(hotCacheLoad, randomCacheLoad, "before previous page (page2.start) request");
        PageInfo page1 = bookInfo.getPreviousPage(PAGE_2.getStarted());
        assertNotNull(page1);
        assertEquals(page1.getName(), PAGE_1.getName());
        assertLoadCount(hotCacheLoad, randomCacheLoad, "after previous page (page2.start) request");

        assertLoadCount(hotCacheLoad, randomCacheLoad, "before previous page (page1.start) request");
        assertNull(bookInfo.getPreviousPage(PAGE_1.getStarted()));
        assertLoadCount(hotCacheLoad, randomCacheLoad, "after previous page (page1.start) request");
    }

    @Test(description = "Book info refresh")
    public void refreshTest() {
        for (int i = 0; i < 5; i++) {
            assertLoadCount(hotCacheLoad, randomCacheLoad, "before book refresh");
            bookInfo.refresh();
            assertLoadCount(hotCacheLoad += 2, randomCacheLoad, "after book refresh");
        }
    }

    @Test(description = "Book info refresh")
    public void findNonExistedPage() {
        for (int i = 0; i < 5; i++) {
            assertLoadCount(hotCacheLoad, randomCacheLoad, "before find nonexistent page, iteration: " + i);
            assertNull(bookInfo.findPage(NOW.minus(4, DAYS)));
            assertLoadCount(hotCacheLoad, randomCacheLoad += 1, "after find nonexistent page, iteration: " + i);
        }
    }

    @Test(description = "Book info refresh")
    public void findExistedPage() {
        assertLoadCount(hotCacheLoad, randomCacheLoad, "before find page in hot cache");
        assertEquals(bookInfo.findPage(NOW).getName(), PAGE_1.getName());
        assertLoadCount(hotCacheLoad, randomCacheLoad, "after find page in hot cache");

        assertLoadCount(hotCacheLoad, randomCacheLoad, "before first find page out of hot cache");
        assertEquals(bookInfo.findPage(NOW.plus(4, DAYS)).getName(), PAGE_4.getName());
        assertLoadCount(hotCacheLoad, randomCacheLoad += 1, "after first find page out of hot cache");

        for (int i = 0; i < 5; i++) {
            assertLoadCount(hotCacheLoad, randomCacheLoad, "before find page out of hot cache, iteration: " + i);
            assertEquals(bookInfo.findPage(NOW.plus(4, DAYS)).getName(), PAGE_4.getName());
            assertLoadCount(hotCacheLoad, randomCacheLoad, "after find page out of hot cache, iteration: " + i);
        }
    }

    @DataProvider(name = "orders")
    public Order[] orders() {
        return Order.values();
    }

    private void assertLoadCount(double expectedHot, double expectedRandom, String comment) {
        assertEquals(getLoadCount(BOOK_ID, HOT), expectedHot, HOT + " cache " + comment);
        assertEquals(getLoadCount(BOOK_ID, RANDOM), expectedRandom, RANDOM + " cache " + comment);
    }

    private List<String> extractNames(Iterator<PageInfo> iterator, Order order) {
        List<String> list = new ArrayList<>();
        iterator.forEachRemaining( pageInfo -> list.add(pageInfo.getName()));
        switch (order) {
            case DIRECT:
                // do nothing
                break;
            case REVERSE:
                Collections.reverse(list);
                break;
            default:
                throw new IllegalStateException("Unknown order " + order);
        }
        return list;
    }

    private List<String> extractNames(Iterator<PageInfo> iterator) {
        return extractNames(iterator, DIRECT);
    }
}

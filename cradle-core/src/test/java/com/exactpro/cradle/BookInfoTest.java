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
package com.exactpro.cradle;

import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nullable;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import static com.exactpro.cradle.Order.DIRECT;
import static com.exactpro.cradle.Order.REVERSE;
import static com.google.common.collect.Lists.newArrayList;
import static java.time.temporal.ChronoUnit.DAYS;
import static java.time.temporal.ChronoUnit.HOURS;
import static java.time.temporal.ChronoUnit.NANOS;
import static java.util.Collections.emptyList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;

public class BookInfoTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(BookInfoTest.class);
    public static Random RANDOM = new Random();
    public static final BookId BOOK_ID = new BookId("test-book");
    private static final List<PageInfo> PAGES;

    static {
        List<PageInfo> pages = new ArrayList<>();
        Instant start = Instant.now().minus(7, ChronoUnit.DAYS);
        Instant end = Instant.now();
        Instant current = start;
        Instant previous;
        do {
            previous = current;
            current = current.plus(1, ChronoUnit.HOURS);
            pages.add(createPageInfo(previous, current));
        } while (current.isBefore(end));
        pages.add(createPageInfo(current, null));

        // Add random gap
        int index = RANDOM.nextInt(pages.size() - 2) + 1;
        PageInfo pageInfo = pages.remove(index);

        PAGES = Collections.unmodifiableList(pages);
        LOGGER.info("Pages - min: {}, gap: {}:{}, max: {}, size: {}",
                PAGES.get(0).getStarted(),
                index, pageInfo.getStarted(),
                PAGES.get(PAGES.size() - 1).getStarted(),
                PAGES.size()
        );
    }

    @Test(dataProvider = "cacheSize")
    public void lazyPageAddTest(int cacheSize) {
        List<PageInfo> operateSource = new ArrayList<>();
        BookInfo bookInfo = createBookInfo(operateSource, cacheSize);

        assertNull(bookInfo.getFirstPage());
        assertNull(bookInfo.getLastPage());
        assertEquals(bookInfo.getPages(), emptyList());

        for (int addIndex = 0; addIndex < PAGES.size(); addIndex++) {
            PageInfo newPage = PAGES.get(addIndex);
            operateSource.add(newPage);
            bookInfo.refresh();

            assertSame(bookInfo.getFirstPage(), PAGES.get(0), "iteration - " + addIndex);
            assertSame(bookInfo.getLastPage(), newPage, "iteration - " + addIndex);
            assertEquals(bookInfo.getPages(), PAGES.subList(0, addIndex + 1), "iteration - " + addIndex);

            for (int checkIndex = addIndex; checkIndex >= 0; checkIndex--) {
                PageInfo source = PAGES.get(checkIndex);
                assertSame(bookInfo.getPage(source.getId()), source, "iteration - " + addIndex + '.' + checkIndex);
                assertSame(bookInfo.findPage(source.getId().getStart()), source, "iteration - " + addIndex + '.' + checkIndex);

                if (source.getEnded() == null) {
                    assertSame(bookInfo.findPage(Instant.MAX), source, "iteration - " + addIndex + '.' + checkIndex);
                } else {
                    assertSame(bookInfo.findPage(source.getEnded().minus(1, NANOS)), source, "iteration - " + addIndex + '.' + checkIndex);
                }

                if (checkIndex > 0) {
                    assertSame(bookInfo.getPreviousPage(source.getId().getStart()), PAGES.get(checkIndex - 1), "iteration - " + addIndex + '.' + checkIndex);
                } else {
                    assertNull(bookInfo.getPreviousPage(source.getId().getStart()), "iteration - " + addIndex + '.' + checkIndex + ", timestamp: " + source.getId().getStart());
                }

                if (checkIndex < addIndex) {
                    assertSame(bookInfo.getNextPage(source.getId().getStart()), PAGES.get(checkIndex + 1), "iteration - " + addIndex + '.' + checkIndex);
                } else {
                    assertNull(bookInfo.getNextPage(source.getId().getStart()), "iteration - " + addIndex + '.' + checkIndex + ", timestamp: " + source.getId().getStart());
                }
            }
        }
    }

    @Test(dataProvider = "orders")
    public void getAllPagesTest(Order order) {
        List<PageInfo> operateSource = new ArrayList<>(PAGES);
        BookInfo bookInfo = createBookInfo(operateSource, 1);

        Iterator<PageInfo> iterator = bookInfo.getPages(null, null, order);
        assertEquals(newArrayList(iterator), optionalReverse(order, operateSource));
    }

    @Test(dataProvider = "orderToPages")
    public void getPagesTest(Order order, List<Instant> pageTimestamps) {
        Instant time1 = pageTimestamps.get(0);
        Instant time2 = pageTimestamps.get(1);
        Instant time3 = pageTimestamps.get(2);
        Instant time4 = pageTimestamps.get(3);
        List<PageInfo> operateSource = List.of(
                createPageInfo(time1, time2),
                createPageInfo(time2, time3),
                createPageInfo(time3, time4)
        );
        BookInfo bookInfo = createBookInfo(operateSource, 1);

        assertEquals(
                newArrayList(bookInfo.getPages(time2.minus(1, NANOS), time3.minus(1, NANOS), order)),
                optionalReverse(order, operateSource.subList(0,2)),
                "Pages where start (-1) to end (-1) timestamps"
        );
        assertEquals(
                newArrayList(bookInfo.getPages(time2, time3.minus(1, NANOS), order)),
                optionalReverse(order, operateSource.subList(1,2)),
                "Pages where start (0) to end (-1) timestamps"
        );
        assertEquals(
                newArrayList(bookInfo.getPages(time2.plus(1, NANOS), time3.minus(1, NANOS), order)),
                optionalReverse(order, operateSource.subList(1,2)),
                "Pages where start (+1) to end (-1) timestamps"
        );

        assertEquals(
                newArrayList(bookInfo.getPages(time2.minus(1, NANOS), time3, order)),
                optionalReverse(order, operateSource.subList(0,3)),
                "Pages where start (-1) to end (0) timestamps"
        );
        assertEquals(
                newArrayList(bookInfo.getPages(time2, time3, order)),
                optionalReverse(order, operateSource.subList(1,3)),
                "Pages where start (0) to end (0) timestamps"
        );
        assertEquals(
                newArrayList(bookInfo.getPages(time2.plus(1, NANOS), time3, order)),
                optionalReverse(order, operateSource.subList(1,3)),
                "Pages where start (+1) to end (0) timestamps"
        );

        assertEquals(
                newArrayList(bookInfo.getPages(time2.minus(1, NANOS), time3.plus(1, NANOS), order)),
                optionalReverse(order, operateSource.subList(0,3)),
                "Pages where start (-1) to end (+1) timestamps"
        );
        assertEquals(
                newArrayList(bookInfo.getPages(time2, time3.plus(1, NANOS), order)),
                optionalReverse(order, operateSource.subList(1,3)),
                "Pages where start (0) to end (-1) timestamps"
        );
        assertEquals(
                newArrayList(bookInfo.getPages(time2.plus(1, NANOS), time3.plus(1, NANOS), order)),
                optionalReverse(order, operateSource.subList(1,3)),
                "Pages where start (+1) to end (+1) timestamps"
        );
    }

    @Test(dataProvider = "orderToPages")
    public void getPagesWhenBookHasGapTest(Order order, List<Instant> pageTimestamps) {
        Instant time1 = pageTimestamps.get(0);
        Instant time2 = pageTimestamps.get(1);
        Instant time3 = pageTimestamps.get(2);
        Instant time4 = pageTimestamps.get(3);
        List<PageInfo> operateSource = List.of(
                createPageInfo(time1, time2),
                createPageInfo(time3, time4)
        );
        BookInfo bookInfo = createBookInfo(operateSource, 1);

        assertEquals(
                newArrayList(bookInfo.getPages(time2.minus(1, NANOS), time3.minus(1, NANOS), order)),
                optionalReverse(order, operateSource.subList(0,1)),
                "Pages where start (-1) to end (-1) timestamps"
        );
        assertEquals(
                newArrayList(bookInfo.getPages(time2, time3.minus(1, NANOS), order)),
                emptyList(),
                "Pages where start (0) to end (-1) timestamps"
        );
        assertEquals(
                newArrayList(bookInfo.getPages(time2.plus(1, NANOS), time3.minus(1, NANOS), order)),
                emptyList(),
                "Pages where start (+1) to end (-1) timestamps"
        );

        assertEquals(
                newArrayList(bookInfo.getPages(time2.minus(1, NANOS), time3, order)),
                optionalReverse(order, operateSource.subList(0,2)),
                "Pages where start (-1) to end (0) timestamps"
        );
        assertEquals(
                newArrayList(bookInfo.getPages(time2, time3, order)),
                optionalReverse(order, operateSource.subList(1,2)),
                "Pages where start (0) to end (0) timestamps"
        );
        assertEquals(
                newArrayList(bookInfo.getPages(time2.plus(1, NANOS), time3, order)),
                optionalReverse(order, operateSource.subList(1,2)),
                "Pages where start (+1) to end (0) timestamps"
        );

        assertEquals(
                newArrayList(bookInfo.getPages(time2.minus(1, NANOS), time3.plus(1, NANOS), order)),
                optionalReverse(order, operateSource.subList(0,2)),
                "Pages where start (-1) to end (+1) timestamps"
        );
        assertEquals(
                newArrayList(bookInfo.getPages(time2, time3.plus(1, NANOS), order)),
                optionalReverse(order, operateSource.subList(1,2)),
                "Pages where start (0) to end (-1) timestamps"
        );
        assertEquals(
                newArrayList(bookInfo.getPages(time2.plus(1, NANOS), time3.plus(1, NANOS), order)),
                optionalReverse(order, operateSource.subList(1,2)),
                "Pages where start (+1) to end (+1) timestamps"
        );
    }

    @Test(dataProvider = "orders")
    public void getPagesWithEmptyResult(Order order) {
        PageInfo pageInfo = PAGES.get(0);
        List<PageInfo> operateSource = List.of(pageInfo);
        BookInfo bookInfo = createBookInfo(operateSource, 1);

        assertEquals(
                newArrayList(bookInfo.getPages(null, pageInfo.getStarted().minus(1, NANOS), order)),
                emptyList(),
                "End timestamp before first page start"
        );
        assertEquals(
                newArrayList(bookInfo.getPages(pageInfo.getEnded().plus(1, NANOS), null, order)),
                emptyList(),
                "Start timestamp after last page end"
        );
        assertEquals(
                newArrayList(bookInfo.getPages(pageInfo.getEnded(), pageInfo.getStarted(), order)),
                emptyList(),
                "Start > end timestamp after last page end"
        );
    }

    @Test(dataProvider = "orders")
    // ... ps   |      |   ps  ps ...
    public void getPagesByPointTest(Order order) {
        Instant time1 = Instant.parse("2024-02-13T12:00:00Z");
        Instant time2 = Instant.parse("2024-02-15T12:00:00Z");
        Instant time3 = Instant.parse("2024-02-15T18:00:00Z");
        List<PageInfo> operateSource = List.of(
                createPageInfo(time1, time2),
                createPageInfo(time2, time3),
                createPageInfo(time3, null)
        );
        BookInfo bookInfo = createBookInfo(operateSource, 1);

        assertEquals(
                newArrayList(bookInfo.getPages(Instant.MIN, Instant.MIN, order)),
                emptyList(),
                "Point with min timestamp"
        );
        assertEquals(
                newArrayList(bookInfo.getPages(time1.minus(1, NANOS), time1.minus(1, NANOS), order)),
                emptyList(),
                "Point before first page start"
        );
        assertEquals(
                newArrayList(bookInfo.getPages(time1, time1, order)),
                operateSource.subList(0, 1),
                "Point equals first page start"
        );
        assertEquals(
                newArrayList(bookInfo.getPages(time1.plus(1, NANOS), time1.plus(1, NANOS), order)),
                operateSource.subList(0, 1),
                "Point after first page start"
        );
        assertEquals(
                newArrayList(bookInfo.getPages(time1.plus(1, DAYS), time1.plus(1, DAYS), order)),
                operateSource.subList(0, 1),
                "Point in the middle of first page"
        );
        assertEquals(
                newArrayList(bookInfo.getPages(time2.minus(1, NANOS), time2.minus(1, NANOS), order)),
                operateSource.subList(0, 1),
                "Point before first page end"
        );
        assertEquals(
                newArrayList(bookInfo.getPages(time2, time2, order)),
                operateSource.subList(1, 2),
                "Point equals second page start"
        );
        assertEquals(
                newArrayList(bookInfo.getPages(time3, time3, order)),
                operateSource.subList(2, 3),
                "Point equals third page start"
        );
        assertEquals(
                newArrayList(bookInfo.getPages(Instant.MAX, Instant.MAX, order)),
                operateSource.subList(2, 3),
                "Point with max timestamp"
        );
    }

    @Test
    public void findPageTest() {
        Instant time1 = Instant.parse("2024-02-13T12:00:00Z");
        Instant time2 = Instant.parse("2024-02-15T12:00:00Z");
        Instant time3 = Instant.parse("2024-02-15T18:00:00Z");
        List<PageInfo> operateSource = List.of(
                createPageInfo(time1, time2),
                createPageInfo(time2, time3),
                createPageInfo(time3, null)
        );
        BookInfo bookInfo = createBookInfo(operateSource, 1);

        assertNull(
                bookInfo.findPage(Instant.MIN),
                "Min timestamp"
        );
        assertNull(
                bookInfo.findPage(time1.minus(1, NANOS)),
                "Timestamp before first page start"
        );
        assertEquals(
                bookInfo.findPage(time1),
                operateSource.get(0),
                "Timestamp equals first page start"
        );
        assertEquals(
                bookInfo.findPage(time1.plus(1, NANOS)),
                operateSource.get(0),
                "Timestamp after first page start"
        );
        assertEquals(
                bookInfo.findPage(time1.plus(1, DAYS)),
                operateSource.get(0),
                "Timestamp in the middle of first page"
        );
        assertEquals(
                bookInfo.findPage(time2.minus(1, NANOS)),
                operateSource.get(0),
                "Timestamp before first page end"
        );
        assertEquals(
                bookInfo.findPage(time2),
                operateSource.get(1),
                "Timestamp equals second page start"
        );
        assertEquals(
                bookInfo.findPage(time3),
                operateSource.get(2),
                "Timestamp equals third page start"
        );
        assertEquals(
                bookInfo.findPage(Instant.MAX),
                operateSource.get(2),
                "Max timestamp"
        );
    }

    @Test(description = "Missed page is between to pages where - sP1 ... | ... eP1 ... gap ... sP2 ... | ")
    public void findPageWhenBookHasGapTest1() {
        Instant time1 = Instant.parse("2024-02-14T12:00:00Z");
        Instant time2 = Instant.parse("2024-02-15T12:00:00Z");
        // Gap [2024-02-15T12:00:00Z - 2024-02-15T18:00:00Z)
        Instant time3 = Instant.parse("2024-02-15T18:00:00Z");
        List<PageInfo> operateSource = List.of(
                createPageInfo(time1, time2),
                createPageInfo(time3, null)
        );
        BookInfo bookInfo = createBookInfo(operateSource, 1);

        assertEquals(
                bookInfo.findPage(time2.minus(1, NANOS)),
                operateSource.get(0),
                "Timestamp before first page end"
        );
        assertNull(
                bookInfo.findPage(time2),
                "Timestamp equals missed page start"
        );
        assertNull(
                bookInfo.findPage(time2.plus(1, HOURS)),
                "Timestamp equals missed page start"
        );
        assertEquals(
                bookInfo.findPage(time3),
                operateSource.get(1),
                "Timestamp equals second page start"
        );
    }

    @Test(description = "Missed page is between to pages where - | ... sP1 ... eP1 ... gap ... sP2 ... | ")
    public void findPageWhenBookHasGapTest2() {
        Instant time1 = Instant.parse("2024-02-15T11:00:00Z");
        Instant time2 = Instant.parse("2024-02-15T12:00:00Z");
        // Gap [2024-02-15T12:00:00Z - 2024-02-15T18:00:00Z)
        Instant time3 = Instant.parse("2024-02-15T18:00:00Z");
        List<PageInfo> operateSource = List.of(
                createPageInfo(time1, time2),
                createPageInfo(time3, null)
        );
        BookInfo bookInfo = createBookInfo(operateSource, 1);

        assertEquals(
                bookInfo.findPage(time2.minus(1, NANOS)),
                operateSource.get(0),
                "Timestamp before first page end"
        );
        assertNull(
                bookInfo.findPage(time2),
                "Timestamp equals missed page start"
        );
        assertNull(
                bookInfo.findPage(time2.plus(1, HOURS)),
                "Timestamp equals missed page start"
        );
        assertEquals(
                bookInfo.findPage(time3),
                operateSource.get(1),
                "Timestamp equals second page start"
        );
    }

    @Test(description = "Missed page is between to pages where - sP1 ... eP1 ... | ... gap ... sP2 ... | ")
    public void findPageWhenBookHasGapTest3() {
        Instant time1 = Instant.parse("2024-02-13T11:00:00Z");
        Instant time2 = Instant.parse("2024-02-13T12:00:00Z");
        // Gap [2024-02-13T12:00:00Z - 2024-02-15T18:00:00Z)
        Instant time3 = Instant.parse("2024-02-15T18:00:00Z");
        List<PageInfo> operateSource = List.of(
                createPageInfo(time1, time2),
                createPageInfo(time3, null)
        );
        BookInfo bookInfo = createBookInfo(operateSource, 1);

        assertEquals(
                bookInfo.findPage(time2.minus(1, NANOS)),
                operateSource.get(0),
                "Timestamp before first page end"
        );
        assertNull(
                bookInfo.findPage(time2),
                "Timestamp equals missed page start"
        );
        assertNull(
                bookInfo.findPage(time2.plus(1, DAYS)),
                "Timestamp equals missed page start"
        );
        assertEquals(
                bookInfo.findPage(time3),
                operateSource.get(1),
                "Timestamp equals second page start"
        );
    }

    @Test
    public void removePageTest() {
        List<PageInfo> operateSource = new ArrayList<>(PAGES);
        BookInfo bookInfo = createBookInfo(operateSource, 1);

        assertEquals(bookInfo.getPages(), operateSource);
        int iteration = 0;
        while (!operateSource.isEmpty()) {
            iteration++;
            int index = RANDOM.nextInt(operateSource.size());
            PageInfo pageForRemove = operateSource.get(index);

            assertSame(bookInfo.getPage(pageForRemove.getId()), pageForRemove, "iteration - " + iteration);
            assertSame(bookInfo.findPage(pageForRemove.getId().getStart()), pageForRemove, "iteration - " + iteration);
            assertEquals(bookInfo.getPages(), operateSource, "iteration - " + iteration);

            operateSource.remove(index);
            bookInfo.invalidate(pageForRemove.getStarted());

            assertNull(bookInfo.getPage(pageForRemove.getId()), "iteration - " + iteration);

            for (PageInfo page : operateSource) {
                assertSame(bookInfo.getPage(page.getId()), page, "iteration - " + iteration);
                assertSame(bookInfo.findPage(page.getId().getStart()), page, "iteration - " + iteration);
            }
        }
    }

    @Test
    public void addPageTest() {
        ArrayList<PageInfo> pages = new ArrayList<>();
        BookInfo bookInfo = createBookInfo(pages, 1);

        assertNull(bookInfo.getFirstPage());
        assertNull(bookInfo.getLastPage());

        for (int i = 0; i < PAGES.size(); i++) {
            PageInfo page = PAGES.get(i);
            pages.add(page);
            bookInfo.invalidate(page.getStarted());

            assertSame(bookInfo.getFirstPage(), pages.get(0), "iteration - " + i);
            assertSame(bookInfo.getLastPage(), pages.get(pages.size() - 1), "iteration - " + i);
            assertSame(bookInfo.getPage(page.getId()), page, "iteration - " + i);
            assertSame(bookInfo.findPage(page.getId().getStart()), page, "iteration - " + i);
        }
    }

    private static <T> List<T> optionalReverse(Order order, List<T> origin) {
        if (order == DIRECT) {
            return origin;
        }
        return Lists.reverse(origin);
    }

    private static BookInfo createBookInfo(List<PageInfo> pages, int cacheSize) {
        return new BookInfo(
                BOOK_ID,
                "test-full-name",
                "test-description",
                Instant.EPOCH,
                cacheSize,
                new TestPagesLoader(pages),
                new TestPageLoader(pages, true),
                new TestPageLoader(pages, false)
        );
    }

    private static PageInfo createPageInfo(Instant start, @Nullable Instant end) {
        return new PageInfo(new PageId(BOOK_ID, start, start.toString()), end, "test-comment");
    }

    @DataProvider(name = "orders")
    public Order[] orders() {
        return Order.values();
    }

    @DataProvider(name = "orderToPages")
    public Object[][] orderToPages() {
        // ... sP1 ... eP1 [sP2 ... eP2) sP3 ... eP3 ...
        List<Instant> case1 = List.of(
                Instant.parse("2024-02-13T01:01:00Z"),
                Instant.parse("2024-02-13T12:00:00Z"),
                Instant.parse("2024-02-13T13:00:00Z"),
                Instant.parse("2024-02-13T23:00:00Z")
        );

        // ... sP1 ... | ... eP1 [sP2 ... eP2) sP3 ... eP3 ...
        List<Instant> case2 = List.of(
                Instant.parse("2024-02-12T01:02:00Z"),
                Instant.parse("2024-02-13T12:00:00Z"),
                Instant.parse("2024-02-15T13:00:00Z"),
                Instant.parse("2024-02-15T23:00:00Z")
        );
        // ... sP1 ... eP1 [sP2 ... eP2) sP3 ... | ... eP3 ...
        List<Instant> case3 = List.of(
                Instant.parse("2024-02-13T01:03:00Z"),
                Instant.parse("2024-02-13T12:00:00Z"),
                Instant.parse("2024-02-13T13:00:00Z"),
                Instant.parse("2024-02-14T23:00:00Z")
        );

        // ... sP1 ... | ... | ... eP1 [sP2 ... eP2) sP3 ... eP3 ...
        List<Instant> case4 = List.of(
                Instant.parse("2024-02-11T01:04:00Z"),
                Instant.parse("2024-02-13T12:00:00Z"),
                Instant.parse("2024-02-13T13:00:00Z"),
                Instant.parse("2024-02-13T23:00:00Z")
        );
        // ... sP1 ... eP1 [sP2 ... eP2) sP3 ... | ... | ... eP3 ...
        List<Instant> case5 = List.of(
                Instant.parse("2024-02-13T01:05:00Z"),
                Instant.parse("2024-02-13T12:00:00Z"),
                Instant.parse("2024-02-13T13:00:00Z"),
                Instant.parse("2024-02-15T23:00:00Z")
        );

        // ... sP1 ... eP1 [sP2 ... | ... eP2) sP3 ... eP3 ...
        List<Instant> case6 = List.of(
                Instant.parse("2024-02-13T01:06:00Z"),
                Instant.parse("2024-02-13T12:00:00Z"),
                Instant.parse("2024-02-14T13:00:00Z"),
                Instant.parse("2024-02-14T23:00:00Z")
        );

        return new Object[][] {
                {DIRECT, case1},
                {DIRECT, case2},
                {DIRECT, case3},
                {DIRECT, case4},
                {DIRECT, case5},
                {DIRECT, case6},
                {REVERSE, case1},
                {REVERSE, case2},
                {REVERSE, case3},
                {REVERSE, case4},
                {REVERSE, case5},
                {REVERSE, case6},
        };
    }

    @DataProvider(name = "cacheSize")
    public static Integer[] cacheSize() {
        return new Integer[]{1, 5, 10};
    }
}

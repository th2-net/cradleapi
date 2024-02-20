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

import static java.time.temporal.ChronoUnit.DAYS;
import static java.time.temporal.ChronoUnit.NANOS;
import static java.util.Collections.emptyList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;

public class BookInfoTest {
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

        PAGES = Collections.unmodifiableList(pages);
    }

    @Test
    public void lazyPageAddTest() {
        List<PageInfo> operateSource = new ArrayList<>();
        BookInfo bookInfo = createBookInfo(operateSource);

        assertNull(bookInfo.getFirstPage());
        assertNull(bookInfo.getLastPage());
        assertEquals(bookInfo.getPages(), emptyList());

        for (int addIndex = 0; addIndex < PAGES.size(); addIndex++) {
            PageInfo newPage = PAGES.get(addIndex);
            operateSource.add(newPage);
            bookInfo.invalidate();

            assertSame(bookInfo.getFirstPage(), PAGES.get(0), "iteration - " + addIndex);
            assertSame(bookInfo.getLastPage(), newPage, "iteration - " + addIndex);
            assertEquals(bookInfo.getPages(), PAGES.subList(0, addIndex + 1), "iteration - " + addIndex);

            for (int checkIndex = addIndex; checkIndex >= 0; checkIndex--) {
                PageInfo source = PAGES.get(checkIndex);
                assertSame(bookInfo.getPage(source.getId()), source, "iteration - " + addIndex + '.' + checkIndex);
                assertSame(bookInfo.findPage(source.getId().getStart()), source, "iteration - " + addIndex + '.' + checkIndex);

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

    @Test
    public void getAllPagesInDirectOrderTest() {
        List<PageInfo> operateSource = new ArrayList<>(PAGES);
        BookInfo bookInfo = createBookInfo(operateSource);

        Iterator<PageInfo> iterator = bookInfo.getPages(null, null, Order.DIRECT);
        assertEquals(Lists.newArrayList(iterator), operateSource);
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
        BookInfo bookInfo = createBookInfo(operateSource);

        assertEquals(
                Lists.newArrayList(bookInfo.getPages(time1.minus(1, NANOS), time1.minus(1, NANOS), order)),
                emptyList(),
                "Point before first page start"
        );
        assertEquals(
                Lists.newArrayList(bookInfo.getPages(time1, time1, order)),
                operateSource.subList(0, 1),
                "Point equals first page start"
        );
        assertEquals(
                Lists.newArrayList(bookInfo.getPages(time1.plus(1, NANOS), time1.plus(1, NANOS), order)),
                operateSource.subList(0, 1),
                "Point after first page start"
        );
        assertEquals(
                Lists.newArrayList(bookInfo.getPages(time1.plus(1, DAYS), time1.plus(1, DAYS), order)),
                operateSource.subList(0, 1),
                "Point in the middle of first page"
        );
        assertEquals(
                Lists.newArrayList(bookInfo.getPages(time2.minus(1, NANOS), time2.minus(1, NANOS), order)),
                operateSource.subList(0, 1),
                "Point before first page end"
        );
        assertEquals(
                Lists.newArrayList(bookInfo.getPages(time2, time2, order)),
                operateSource.subList(1, 2),
                "Point equals second page start"
        );
    }

    @Test
    public void getAllPagesInReverseOrderTest() {
        List<PageInfo> source = new ArrayList<>(PAGES);
        BookInfo bookInfo = createBookInfo(source);

        Iterator<PageInfo> iterator = bookInfo.getPages(null, null, Order.REVERSE);
        assertEquals(Lists.newArrayList(iterator), Lists.reverse(source));
    }


    @Test
    public void removePageTest() {
        List<PageInfo> operateSource = new ArrayList<>(PAGES);
        BookInfo bookInfo = createBookInfo(operateSource);

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
        BookInfo bookInfo = createBookInfo(pages);

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

    private static BookInfo createBookInfo(List<PageInfo> pages) {
        return new BookInfo(
                BOOK_ID,
                "test-full-name",
                "test-description",
                Instant.EPOCH,
                1,
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
}

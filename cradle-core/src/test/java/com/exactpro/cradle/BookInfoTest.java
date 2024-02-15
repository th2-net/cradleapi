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

import org.testng.annotations.Test;

import javax.annotation.Nullable;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;

public class BookInfoTest {
    public static Random RANDOM = new Random();
    public static final BookId BOOK_ID = new BookId("test-book");
    public static final List<PageInfo> PAGES;

    static {
        PAGES = new ArrayList<>();
        Instant start = Instant.now().minus(7, ChronoUnit.DAYS);
        Instant end = Instant.now();
        Instant current = start;
        Instant previous;
        do {
            previous = current;
            current = current.plus(1, ChronoUnit.HOURS);
            PAGES.add(createPageInfo(previous, current));
        } while (current.isBefore(end));
        PAGES.add(createPageInfo(current, null));
    }

    @Test
    public void lazyPageAddTest() {
        List<PageInfo> operateSource = new ArrayList<>();
        BookInfo bookInfo = createBookInfo(operateSource);

        assertNull(bookInfo.getFirstPage());
        assertNull(bookInfo.getLastPage());

        for (int i = 0; i < PAGES.size(); i++) {
            operateSource.add(PAGES.get(i));
            bookInfo.invalidate();

            assertSame(bookInfo.getFirstPage(), PAGES.get(0), "iteration - " + i);
            assertSame(bookInfo.getLastPage(), PAGES.get(i), "iteration - " + i);

            for (int j = i; j >= 0; j--) {
                PageInfo source = PAGES.get(j);
                assertSame(bookInfo.getPage(source.getId()), source, "iteration - " + i + '.' + j);
                assertSame(bookInfo.findPage(source.getId().getStart()), source, "iteration - " + i + '.' + j);

                if (j > 0) {
                    assertSame(bookInfo.getPreviousPage(source.getId().getStart()), PAGES.get(j - 1), "iteration - " + i + '.' + j);
                } else {
                    assertNull(bookInfo.getPreviousPage(source.getId().getStart()), "iteration - " + i + '.' + j + ", timestamp: " + source.getId().getStart());
                }
                if (j < i) {
                    assertSame(bookInfo.getNextPage(source.getId().getStart()), PAGES.get(j + 1), "iteration - " + i + '.' + j);
                } else {
                    assertNull(bookInfo.getNextPage(source.getId().getStart()), "iteration - " + i + '.' + j + ", timestamp: " + source.getId().getStart());
                }
            }
        }
    }

    @Test
    public void removePageTest() {
        List<PageInfo> operateSource = new ArrayList<>(PAGES);
        BookInfo bookInfo = createBookInfo(operateSource);

        int iteration = 0;
        while (!operateSource.isEmpty()) {
            iteration++;
            int index = RANDOM.nextInt(operateSource.size());
            PageInfo pageForRemove = operateSource.get(index);

            assertSame(bookInfo.getPage(pageForRemove.getId()), pageForRemove, "iteration - " + iteration);
            assertSame(bookInfo.findPage(pageForRemove.getId().getStart()), pageForRemove, "iteration - " + iteration);

            operateSource.remove(index);
            bookInfo.removePage(pageForRemove.getId());

            assertNull(bookInfo.getPage(pageForRemove.getId()), "iteration - " + iteration);

            for (PageInfo page : operateSource) {
                assertSame(bookInfo.getPage(page.getId()), page, "iteration - " + iteration);
                assertSame(bookInfo.findPage(page.getId().getStart()), page, "iteration - " + iteration);
            }
        }
    }

    @Test
    public void addPageTest() {
        BookInfo bookInfo = createBookInfo(new ArrayList<>());

        assertNull(bookInfo.getFirstPage());
        assertNull(bookInfo.getLastPage());

        for (int i = 0; i < PAGES.size(); i++) {
            bookInfo.addPage(PAGES.get(i));

            PageInfo page = PAGES.get(0);
            assertNull(bookInfo.getFirstPage(), "iteration - " + i);
            assertNull(bookInfo.getLastPage(), "iteration - " + i);
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
                (bookId, start, end) -> pages.stream()
                        .filter(page -> !start.isAfter(page.getStarted()) && !end.isBefore(page.getStarted()))
                        .collect(Collectors.toList()),
                bookId -> pages.isEmpty() ? null : pages.get(0),
                bookId -> pages.isEmpty() ? null : pages.get(pages.size() - 1)
        );
    }

    private static PageInfo createPageInfo(Instant start, @Nullable Instant end) {
        return new PageInfo(new PageId(BOOK_ID, start, start.toString()), start, end, "test-comment");
    }
}

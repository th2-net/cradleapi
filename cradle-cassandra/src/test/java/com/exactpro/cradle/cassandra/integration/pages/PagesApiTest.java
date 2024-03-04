/*
 * Copyright 2023-2024 Exactpro (Exactpro Systems Limited)
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
import com.exactpro.cradle.BookToAdd;
import com.exactpro.cradle.PageId;
import com.exactpro.cradle.PageInfo;
import com.exactpro.cradle.cassandra.integration.BaseCradleCassandraTest;
import com.exactpro.cradle.counters.Interval;
import com.exactpro.cradle.utils.CradleStorageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Spliterator;
import java.util.stream.Collectors;

import static com.exactpro.cradle.CoreStorageSettings.PAGE_ACTION_REJECTION_THRESHOLD_FACTOR;
import static com.exactpro.cradle.cassandra.integration.CassandraCradleHelper.BOOK_REFRESH_INTERVAL_MILLIS;
import static java.time.temporal.ChronoUnit.DAYS;
import static java.time.temporal.ChronoUnit.HOURS;
import static java.time.temporal.ChronoUnit.MILLIS;
import static java.time.temporal.ChronoUnit.NANOS;
import static java.util.Spliterators.spliteratorUnknownSize;
import static java.util.stream.StreamSupport.stream;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

public class PagesApiTest extends BaseCradleCassandraTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(PagesApiTest.class);

    @BeforeClass
    public void startUp() throws IOException, InterruptedException, CradleStorageException {
        super.startUp(true);
        generateData();
    }

    @Override
    protected void generateData() throws CradleStorageException, IOException {
        try {
            for (long i = 3; i < 7; i++) {
                Instant start = Instant.now().plus(i, ChronoUnit.MINUTES);
                storage.addPage(
                        bookId,
                        "autoPageNotNull-" + (i - 2),
                        start,
                        "auto page"
                );
            }
        } catch (CradleStorageException | IOException e) {
            LOGGER.error("Error while generating data:", e);
            throw e;
        }
    }

    @Test(description = "Simply gets all pages, filters them using name and their default values are not null")
    public void testNonNullPages() throws CradleStorageException {
        try {
            var result = storage.getAllPages(bookId);
            var autoPagesNonNull = result.stream()
                    .filter(pageInfo -> pageInfo.getName().startsWith("autoPageNotNull-"))
                    .collect(Collectors.toList());
            assertThat(autoPagesNonNull.size()).isEqualTo(4);
            autoPagesNonNull.forEach(pageInfo -> {
                assertThat(pageInfo.getComment()).isNotNull();
                assertThat(pageInfo.getUpdated()).isNotNull();
                assertThat(pageInfo.getRemoved()).isNotNull();
            });
        } catch (CradleStorageException e) {
            LOGGER.error(e.getMessage(), e);
            throw e;
        }
    }

    @Test(description = "Gets pages from the max interval (MIN, MAX), filters them using name and their default values are not null")
    public void testPagesByMaxInterval() throws CradleStorageException {
        try {
            var result = storage.getPages(bookId, new Interval(Instant.MIN, Instant.MAX));
            var autoPagesNonNull = stream(spliteratorUnknownSize(result, Spliterator.ORDERED), false)
                    .filter(pageInfo -> pageInfo.getName().startsWith("autoPageNotNull-"))
                    .collect(Collectors.toList());
            assertThat(autoPagesNonNull.size()).isEqualTo(4);
            autoPagesNonNull.forEach(pageInfo -> {
                assertThat(pageInfo.getComment()).isNotNull();
                assertThat(pageInfo.getUpdated()).isNotNull();
                assertThat(pageInfo.getRemoved()).isNotNull();
            });
        } catch (CradleStorageException e) {
            LOGGER.error(e.getMessage(), e);
            throw e;
        }
    }

    @Test
    public void testFillGapInPenultimatePage() throws CradleStorageException, IOException {
        try {
            // Preparation
            BookId testBookId = new BookId("testFillGapInPenultimatePage");
            Instant testBookStart = Instant.now().minus(10, ChronoUnit.DAYS);
            PageId pageId1 = new PageId(testBookId, Instant.now(), "page1");
            PageId pageId2 = new PageId(testBookId, Instant.now().plus(1, HOURS), "page2");
            PageId pageId3 = new PageId(testBookId, Instant.now().plus(2, HOURS), "page3");
            storage.addBook(new BookToAdd(testBookId.getName(), testBookStart));
            storage.addPage(testBookId, pageId1.getName(), pageId1.getStart(), null);
            storage.addPage(testBookId, pageId2.getName(), pageId2.getStart(), null);
            storage.addPage(testBookId, pageId3.getName(), pageId3.getStart(), null);
            storage.removePage(pageId2);

            var result = storage.getAllPages(testBookId);
            assertEquals(result.size(), 2);
            var pages = toMap(result);

            PageInfo page1 = pages.get(pageId1);
            PageInfo page3 = pages.get(pageId3);
            assertNotNull(page1);
            assertNotNull(page3);
            assertEquals(page1.getEnded(), pageId2.getStart());
            assertNull(page3.getEnded());

            // test
            PageId pageId4 = new PageId(testBookId, pageId2.getStart(), "page4");
            storage.addPage(testBookId, pageId4.getName(), pageId4.getStart(), null);

            result = storage.getAllPages(testBookId);
            assertEquals(result.size(), 3);
            pages = toMap(result);

            page1 = pages.get(pageId1);
            PageInfo page4 = pages.get(pageId4);
            page3 = pages.get(pageId3);
            assertNotNull(page1);
            assertNotNull(pageId4);
            assertNotNull(page3);
            assertEquals(page1.getEnded(), pageId2.getStart());
            assertEquals(page4.getEnded(), pageId3.getStart());
            assertNull(page3.getEnded());

        } catch (IOException | CradleStorageException e) {
            LOGGER.error(e.getMessage(), e);
            throw e;
        }
    }

    @Test(description = "Try to add the second page before page action reject threshold",
            expectedExceptions = CradleStorageException.class,
            expectedExceptionsMessageRegExp = "You can only create pages which start more than.*"
    )
    public void testAddTheSecondPageBeforeThreshold() throws CradleStorageException, IOException {
        try {
            // Preparation
            BookId testBookId = new BookId("testAddPagesToThePast");
            Instant testBookStart = Instant.now().minus(10, ChronoUnit.DAYS);
            // Adding page before book start time looks strange but the current behavior doesn't affect anybody
            PageId pageId1 = new PageId(testBookId, testBookStart.minus(10, DAYS), "page1");
            storage.addBook(new BookToAdd(testBookId.getName(), testBookStart));
            storage.addPage(testBookId, pageId1.getName(), pageId1.getStart(), null);
            assertEquals(storage.getAllPages(testBookId).size(), 1);

            // Test
            PageId pageId2 = new PageId(testBookId,
                    Instant.now().plus(BOOK_REFRESH_INTERVAL_MILLIS * PAGE_ACTION_REJECTION_THRESHOLD_FACTOR,
                            MILLIS),
                    "page2");
            storage.addPage(testBookId, pageId2.getName(), pageId2.getStart(), null);
        } catch (IOException | CradleStorageException e) {
            LOGGER.error(e.getMessage(), e);
            throw e;
        }
    }

    @Test(description = "Try to add page with start timestamp between already existed page",
            expectedExceptions = CradleStorageException.class,
            expectedExceptionsMessageRegExp = "Can't add new page in book.*"
    )
    public void testAddPageInTheMiddleOfExist() throws CradleStorageException, IOException {
        List<PageInfo> allPages = new ArrayList<>(storage.getAllPages(bookId));
        PageInfo pageInfo = allPages.get(allPages.size() - 2);
        storage.addPage(bookId, "invalid-page", pageInfo.getStarted().plus(1, NANOS), null);
    }
}

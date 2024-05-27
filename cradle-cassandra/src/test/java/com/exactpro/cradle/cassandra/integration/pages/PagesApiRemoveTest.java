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
import com.exactpro.cradle.BookToAdd;
import com.exactpro.cradle.PageId;
import com.exactpro.cradle.PageInfo;
import com.exactpro.cradle.PageToAdd;
import com.exactpro.cradle.cassandra.integration.BaseCradleCassandraTest;
import com.exactpro.cradle.cassandra.integration.CassandraCradleHelper;
import com.exactpro.cradle.utils.CradleStorageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static java.time.temporal.ChronoUnit.HOURS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

public class PagesApiRemoveTest extends BaseCradleCassandraTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(PagesApiRemoveTest.class);
    private static final BookId BOOK_ID = new BookId(PagesApiRemoveTest.class.getSimpleName() + "Book");
    private static final Instant BOOK_START = Instant.now().minus(10, ChronoUnit.DAYS);
    private static final List<PageId> PAGE_IDS = List.of(
            new PageId(BOOK_ID, Instant.now().minus(5, ChronoUnit.DAYS), "page1"),
            new PageId(BOOK_ID, Instant.now().minus(4, ChronoUnit.DAYS), "page2"),
            new PageId(BOOK_ID, Instant.now(), "page3"),
            new PageId(BOOK_ID, Instant.now().plus(1, HOURS), "page4"),
            new PageId(BOOK_ID, Instant.now().plus(1, ChronoUnit.DAYS), "page5")
    );

    @BeforeClass
    public void startUp() throws IOException, InterruptedException, CradleStorageException {
        this.session = CassandraCradleHelper.getInstance().getSession();
        this.storage = CassandraCradleHelper.getInstance().getStorage();
        generateData();
    }

    @Override
    protected void generateData() throws CradleStorageException, IOException {
        try {
            storage.addBook(new BookToAdd(BOOK_ID.getName(), BOOK_START));
            storage.addPages(BOOK_ID, PAGE_IDS.stream()
                    .map(id -> new PageToAdd(id.getName(), id.getStart(), null))
                    .collect(Collectors.toList()));
        } catch (CradleStorageException | IOException e) {
            LOGGER.error("Error while generating data:", e);
            throw e;
        }
    }

    @Test(dataProvider = "pageIds")
    public void testRemoveRandomPage(PageId pageId) throws CradleStorageException, IOException {
        try {
            storage.removePage(pageId);

            var result = storage.getAllPages(BOOK_ID);
            var filteredPage = result.stream()
                    .filter(pageInfo -> pageInfo.getId().equals(pageId))
                    .collect(Collectors.toList());

            Instant now = Instant.now();
            if (pageId.getStart().isAfter(now)) {
                assertThat(filteredPage.size()).isEqualTo(0);
            } else {
                assertThat(filteredPage.size()).isEqualTo(1);
                filteredPage.forEach(pageInfo -> assertThat(pageInfo.getRemoved()).isBefore(now));
            }
        } catch (IOException | CradleStorageException e) {
            LOGGER.error(e.getMessage(), e);
            throw e;
        }
    }

    @Test
    public void testRemoveLastPage() throws CradleStorageException, IOException {
        try {
            // Preparation
            BookId testBookId = new BookId("testRemoveLastPage");
            PageId pageId1 = new PageId(testBookId, Instant.now(), "page1");
            PageId pageId2 = new PageId(testBookId, Instant.now().plus(1, HOURS), "page2");
            storage.addBook(new BookToAdd(testBookId.getName(), BOOK_START));
            storage.addPage(testBookId, pageId1.getName(), pageId1.getStart(), null);
            storage.addPage(testBookId, pageId2.getName(), pageId2.getStart(), null);

            var result = storage.getAllPages(testBookId);
            assertEquals(result.size(), 2);
            var pages = toMap(result);

            PageInfo page1 = pages.get(pageId1);
            PageInfo page2 = pages.get(pageId2);
            assertNotNull(page1);
            assertNotNull(page2);
            assertEquals(page1.getEnded(), pageId2.getStart());
            assertNull(page2.getEnded());

            // test
            storage.removePage(pageId2);

            result = storage.getAllPages(testBookId);
            assertEquals(result.size(), 1);
            pages = toMap(result);

            page1 = pages.get(pageId1);
            assertNotNull(page1);

            assertNull(page1.getEnded());

        } catch (IOException | CradleStorageException e) {
            LOGGER.error(e.getMessage(), e);
            throw e;
        }
    }

    @Test
    public void testRemovePenultimatePage() throws CradleStorageException, IOException {
        try {
            // Preparation
            BookId testBookId = new BookId("testRemovePenultimatePage");
            PageId pageId1 = new PageId(testBookId, Instant.now(), "page1");
            PageId pageId2 = new PageId(testBookId, Instant.now().plus(1, HOURS), "page2");
            PageId pageId3 = new PageId(testBookId, Instant.now().plus(2, HOURS), "page3");
            storage.addBook(new BookToAdd(testBookId.getName(), BOOK_START));
            storage.addPage(testBookId, pageId1.getName(), pageId1.getStart(), null);
            storage.addPage(testBookId, pageId2.getName(), pageId2.getStart(), null);
            storage.addPage(testBookId, pageId3.getName(), pageId3.getStart(), null);

            var result = storage.getAllPages(testBookId);
            assertEquals(result.size(), 3);
            var pages = toMap(result);

            PageInfo page1 = pages.get(pageId1);
            PageInfo page2 = pages.get(pageId2);
            PageInfo page3 = pages.get(pageId3);
            assertNotNull(page1);
            assertNotNull(page2);
            assertNotNull(page3);
            assertEquals(page1.getEnded(), pageId2.getStart());
            assertEquals(page2.getEnded(), pageId3.getStart());
            assertNull(page3.getEnded());

            // test
            storage.removePage(pageId2);

            result = storage.getAllPages(testBookId);
            assertEquals(result.size(), 2);
            pages = toMap(result);

            page1 = pages.get(pageId1);
            page3 = pages.get(pageId3);
            assertNotNull(page1);
            assertNotNull(page3);
            assertEquals(page1.getEnded(), pageId2.getStart());
            assertNull(page3.getEnded());

        } catch (IOException | CradleStorageException e) {
            LOGGER.error(e.getMessage(), e);
            throw e;
        }
    }

    @DataProvider(name = "pageIds")
    public static PageId[] cacheSize() {
        List<PageId> ids = new ArrayList<>(PAGE_IDS);
        Collections.shuffle(ids);
        return ids.toArray(new PageId[0]);
    }
}

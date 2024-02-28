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
import com.exactpro.cradle.cassandra.integration.BaseCradleCassandraTest;
import com.exactpro.cradle.cassandra.integration.CassandraCradleHelper;
import com.exactpro.cradle.utils.CradleStorageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

import static java.time.temporal.ChronoUnit.HOURS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

public class PagesApiFillPageGapTest extends BaseCradleCassandraTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(PagesApiFillPageGapTest.class);

    @BeforeClass
    public void startUp() throws IOException, InterruptedException, CradleStorageException {
        this.session = CassandraCradleHelper.getInstance().getSession();
        this.storage = CassandraCradleHelper.getInstance().getStorage();
    }

    @Override
    protected void generateData() throws CradleStorageException, IOException { }

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
}

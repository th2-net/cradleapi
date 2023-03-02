/*
 * Copyright 2020-2023 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.cradle.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.exactpro.cradle.*;
import com.exactpro.cradle.cassandra.keyspaces.TableTruncator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class BaseCassandraTest {

    private static final Logger logger = LoggerFactory.getLogger(BaseCassandraTest.class);


    protected static final String DEFAULT_PAGE_PREFIX = "test_page_";
    protected static final BookId DEFAULT_BOOK_ID = new BookId("test_book");
    private static final Instant DEFAULT_DATA_END = Instant.now();
    private static final Instant DEFAULT_DATA_START = DEFAULT_DATA_END.minus(1, ChronoUnit.HOURS);

    private static final List<PageInfo> DEFAULT_PAGES = List.of(
            new PageInfo(
                    new PageId(DEFAULT_BOOK_ID, DEFAULT_PAGE_PREFIX+0),
                    DEFAULT_DATA_START,
                    DEFAULT_DATA_START.plus(10, ChronoUnit.MINUTES), ""),
            new PageInfo(
                    new PageId(DEFAULT_BOOK_ID, DEFAULT_PAGE_PREFIX+1),
                    DEFAULT_DATA_START.plus(10, ChronoUnit.MINUTES),
                    DEFAULT_DATA_START.plus(20, ChronoUnit.MINUTES), ""),
            new PageInfo(
                    new PageId(DEFAULT_BOOK_ID, DEFAULT_PAGE_PREFIX+2),
                    DEFAULT_DATA_START.plus(20, ChronoUnit.MINUTES),
                    DEFAULT_DATA_START.plus(30, ChronoUnit.MINUTES), ""),
            new PageInfo(
                    new PageId(DEFAULT_BOOK_ID, DEFAULT_PAGE_PREFIX+3),
                    DEFAULT_DATA_START.plus(30, ChronoUnit.MINUTES),
                    DEFAULT_DATA_START.plus(40, ChronoUnit.MINUTES), ""),
            new PageInfo(
                    new PageId(DEFAULT_BOOK_ID, DEFAULT_PAGE_PREFIX+4),
                    DEFAULT_DATA_START.plus(40, ChronoUnit.MINUTES),
                    DEFAULT_DATA_START.plus(50, ChronoUnit.MINUTES), ""),
            new PageInfo(
                    new PageId(DEFAULT_BOOK_ID, DEFAULT_PAGE_PREFIX+5),
                    DEFAULT_DATA_START.plus(50, ChronoUnit.MINUTES),
                    DEFAULT_DATA_START.plus(60, ChronoUnit.MINUTES), ""));


    protected List<PageInfo> pages = DEFAULT_PAGES;
    protected CqlSession session;
    protected CassandraCradleStorage storage;
    protected Instant dataStart = DEFAULT_DATA_START;
    protected Instant dataEnd = DEFAULT_DATA_END;
    protected BookId bookId = DEFAULT_BOOK_ID;

    protected void startUp () {
        startUp(false);
    }

    protected void startUp(boolean initBooksAndPages) {
        TableTruncator.truncateTables(CassandraCradleHelper.getInstance().getSession(), CassandraCradleHelper.KEYSPACE_NAME);


        this.session = CassandraCradleHelper.getInstance().getSession();
        this.storage = CassandraCradleHelper.getInstance().getStorage();

        if (initBooksAndPages) {
            setUpBooksAndPages(
                    DEFAULT_BOOK_ID,
                    DEFAULT_PAGES.stream().map(
                            el -> new PageToAdd(
                                    el.getId().getName(),
                                    el.getStarted(),
                                    el.getComment())).collect(Collectors.toList()));
        }
    }

    protected void setUpBooksAndPages (BookId bookId, List<PageToAdd> pagesToAdd) {
        try {
            storage.addBook(new BookToAdd(bookId.getName(), dataStart));

            BookInfo book = storage.addPages(bookId, pagesToAdd);

            pages = new ArrayList<>(book.getPages());
        } catch (Exception e) {
            logger.error("", e);
        }
    }
}

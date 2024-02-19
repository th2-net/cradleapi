/*
 * Copyright 2020-2024 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.cradle.cassandra.integration;

import com.datastax.oss.driver.api.core.CqlSession;
import com.exactpro.cradle.*;
import com.exactpro.cradle.cassandra.CassandraCradleStorage;
import com.exactpro.cradle.messages.MessageToStore;
import com.exactpro.cradle.testevents.*;
import com.exactpro.cradle.utils.CradleStorageException;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Following class should be extended in order to
 * use tests with embedded cassandra without
 * actually calling any of init or utility methods
 */
public abstract class BaseCradleCassandraTest {
    protected static final String DEFAULT_PAGE_PREFIX = "test_page_";
    protected static final BookId DEFAULT_BOOK_ID = new BookId("test_book");
    private final static String EVENT_NAME = "default_event_name";
    private static final Instant DEFAULT_DATA_END = Instant.now();
    private static final Instant DEFAULT_DATA_START = DEFAULT_DATA_END.minus(1, ChronoUnit.HOURS);
    public static final String protocol = "default_message_protocol";
    public static final String CONTENT = "default_content";

    private final long storeActionRejectionThreshold = new CoreStorageSettings().calculateStoreActionRejectionThreshold();



    private static final List<PageInfo> DEFAULT_PAGES = List.of(
            new PageInfo(
                    new PageId(DEFAULT_BOOK_ID, DEFAULT_DATA_START, DEFAULT_PAGE_PREFIX + 0),
                    DEFAULT_DATA_START.plus(10, ChronoUnit.MINUTES), ""),
            new PageInfo(
                    new PageId(DEFAULT_BOOK_ID, DEFAULT_DATA_START.plus(10, ChronoUnit.MINUTES), DEFAULT_PAGE_PREFIX + 1),
                    DEFAULT_DATA_START.plus(20, ChronoUnit.MINUTES), ""),
            new PageInfo(
                    new PageId(DEFAULT_BOOK_ID, DEFAULT_DATA_START.plus(20, ChronoUnit.MINUTES), DEFAULT_PAGE_PREFIX + 2),
                    DEFAULT_DATA_START.plus(30, ChronoUnit.MINUTES), ""),
            new PageInfo(
                    new PageId(DEFAULT_BOOK_ID, DEFAULT_DATA_START.plus(30, ChronoUnit.MINUTES), DEFAULT_PAGE_PREFIX + 3),
                    DEFAULT_DATA_START.plus(40, ChronoUnit.MINUTES), ""),
            new PageInfo(
                    new PageId(DEFAULT_BOOK_ID, DEFAULT_DATA_START.plus(40, ChronoUnit.MINUTES), DEFAULT_PAGE_PREFIX + 4),
                    DEFAULT_DATA_START.plus(50, ChronoUnit.MINUTES), ""),
            new PageInfo(
                    new PageId(DEFAULT_BOOK_ID, DEFAULT_DATA_START.plus(50, ChronoUnit.MINUTES), DEFAULT_PAGE_PREFIX + 5),
                    DEFAULT_DATA_START.plus(60, ChronoUnit.MINUTES), ""));


    protected List<PageInfo> pages = DEFAULT_PAGES;
    protected CqlSession session;
    protected CassandraCradleStorage storage;
    protected Instant dataStart = DEFAULT_DATA_START;
    protected BookId bookId = DEFAULT_BOOK_ID;

    /*
        Following method should be used in beforeClass if extending class
        wants to implement it's own logic of initializing books and pages
     */
    protected void startUp() throws IOException, InterruptedException, CradleStorageException {
        startUp(false);
    }

    private BookId generateBookId() {
        return new BookId(getClass().getSimpleName() + "Book");
    }

    /**
     * Following method should be implemented and
     * then used in beforeClass. Here should go all data
     * initialization logic for whole class.
     */
    protected abstract void generateData() throws CradleStorageException, IOException;

    protected void startUp(boolean generateBookPages) throws IOException, InterruptedException, CradleStorageException {
        this.session = CassandraCradleHelper.getInstance().getSession();
        this.storage = CassandraCradleHelper.getInstance().getStorage();
        this.bookId = generateBookId();

        if (generateBookPages) {
            setUpBooksAndPages(
                    bookId,
                    DEFAULT_PAGES.stream().map(
                            el -> new PageToAdd(
                                    el.getName(),
                                    el.getId().getStart(),
                                    el.getComment())).collect(Collectors.toList()));
        }
    }

    protected void setUpBooksAndPages(BookId bookId, List<PageToAdd> pagesToAdd) throws CradleStorageException, IOException {
        storage.addBook(new BookToAdd(bookId.getName(), dataStart));

        BookInfo book = storage.addPages(bookId, pagesToAdd);

        pages = new ArrayList<>(book.getPages());
    }

    protected MessageToStore generateMessage(String sessionAlias, Direction direction, int minutesFromStart, long sequence) throws CradleStorageException {
        return MessageToStore.builder()
                .bookId(bookId)
                .sessionAlias(sessionAlias)
                .direction(direction)
                .timestamp(dataStart.plus(minutesFromStart, ChronoUnit.MINUTES))
                .sequence(sequence)
                .content(CONTENT.getBytes(StandardCharsets.UTF_8))
                .protocol(protocol)
                .build();
    }

    /*
    Generates test event filled with events
    each of event having `eventDuration` duration
    while batch itself having `batchDuration` duration
 */
    protected TestEventToStore generateTestEvent (String scope, Instant start, long batchDuration, long eventDuration) throws CradleStorageException {
        StoredTestEventId parentId = new StoredTestEventId(bookId, scope, start, UUID.randomUUID().toString());
        StoredTestEventId id = new StoredTestEventId(bookId, scope, start, UUID.randomUUID().toString());
        TestEventBatchToStore batch = new TestEventBatchToStoreBuilder(100*1024, storeActionRejectionThreshold)
                .name(EVENT_NAME)
                .id(id)
                .parentId(parentId)
                .build();

        for (long i = 0; i < batchDuration; i += eventDuration) {
            batch.addTestEvent(new TestEventSingleToStoreBuilder(storeActionRejectionThreshold)
                    .content(CONTENT.getBytes(StandardCharsets.UTF_8))
                    .id(bookId, scope, start.plusMillis(i), UUID.randomUUID().toString())
                    .endTimestamp(start.plusMillis(i + eventDuration))
                    .success(true)
                    .name(EVENT_NAME)
                    .parentId(parentId)
                    .build());
        }

        return batch;
    }

}

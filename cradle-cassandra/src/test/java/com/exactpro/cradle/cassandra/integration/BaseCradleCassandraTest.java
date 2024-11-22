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
import com.exactpro.cradle.BookInfo;
import com.exactpro.cradle.BookId;
import com.exactpro.cradle.BookToAdd;
import com.exactpro.cradle.PageInfo;
import com.exactpro.cradle.PageId;
import com.exactpro.cradle.PageToAdd;
import com.exactpro.cradle.Direction;
import com.exactpro.cradle.CoreStorageSettings;
import com.exactpro.cradle.cassandra.CassandraCradleStorage;
import com.exactpro.cradle.messages.MessageToStore;
import com.exactpro.cradle.testevents.TestEventToStore;
import com.exactpro.cradle.testevents.TestEventBatchToStore;
import com.exactpro.cradle.testevents.StoredTestEventId;
import com.exactpro.cradle.testevents.TestEventSingleToStoreBuilder;
import com.exactpro.cradle.testevents.TestEventBatchToStoreBuilder;
import com.exactpro.cradle.utils.CradleStorageException;
import org.jetbrains.annotations.NotNull;
import org.testng.annotations.Listeners;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Following class should be extended in order to
 * use tests with embedded cassandra without
 * actually calling any of init or utility methods
 */
@Listeners(TombstoneCounterListener.class)
public abstract class BaseCradleCassandraTest {
    protected static final String DEFAULT_PAGE_PREFIX = "test_page_";
    private final static String EVENT_NAME = "default_event_name";
    public static final String protocol = "default_message_protocol";
    public static final String CONTENT = "default_content";

    private final long storeActionRejectionThreshold = new CoreStorageSettings().calculateStoreActionRejectionThreshold();

    protected final Instant dataStart = Instant.now().minus(70, ChronoUnit.MINUTES);
    protected final BookId bookId = generateBookId();

    protected CqlSession session;
    protected CassandraCradleStorage storage;
    protected List<PageInfo> allPages;
    protected List<PageInfo> activePages;

    private final PageToAdd page_r2 = new PageToAdd(DEFAULT_PAGE_PREFIX + -2, dataStart.minus(20, ChronoUnit.MINUTES), "");
    private final PageToAdd page_r1 = new PageToAdd(DEFAULT_PAGE_PREFIX + -1, dataStart.minus(10, ChronoUnit.MINUTES), "");
    private final PageToAdd page_a0 = new PageToAdd(DEFAULT_PAGE_PREFIX + 0, dataStart, "");
    private final PageToAdd page_a1 = new PageToAdd(DEFAULT_PAGE_PREFIX + 1, dataStart.plus(10, ChronoUnit.MINUTES), "");
    private final PageToAdd page_a2 = new PageToAdd(DEFAULT_PAGE_PREFIX + 2, dataStart.plus(20, ChronoUnit.MINUTES), "");
    private final PageToAdd page_a3 = new PageToAdd(DEFAULT_PAGE_PREFIX + 3, dataStart.plus(30, ChronoUnit.MINUTES), "");
    private final PageToAdd page_a4 = new PageToAdd(DEFAULT_PAGE_PREFIX + 4, dataStart.plus(40, ChronoUnit.MINUTES), "");
    private final PageToAdd page_a5 = new PageToAdd(DEFAULT_PAGE_PREFIX + 5, dataStart.plus(50, ChronoUnit.MINUTES), "");
    private final PageToAdd page_a6 = new PageToAdd(DEFAULT_PAGE_PREFIX + 6, dataStart.plus(60, ChronoUnit.MINUTES), "");

    private final List<PageToAdd> pagesToAdd = List.of(
            page_r2, page_r1,
            page_a0, page_a1, page_a2, page_a3, page_a4, page_a5, page_a6
    );
    private final List<PageId> pageIdToRemove = Stream.of(page_r2, page_r1)
            .map(page -> new PageId(bookId, page.getStart(), page.getName()))
            .collect(Collectors.toList());

    /*
        Following method should be used in beforeClass if extending class
        wants to implement its own logic of initializing books and pages
     */
    protected void startUp() throws IOException, InterruptedException, CradleStorageException {
        startUp(false);
    }
    private BookId generateBookId() {
        return new BookId(getClass().getSimpleName() + "Book");
    }

    @Nonnull
    private List<PageInfo> extractPagesAfterStart(List<PageInfo> allPages, Instant dataStart) {
        return allPages.stream()
                .filter(pageInfo -> !pageInfo.getStarted().isBefore(dataStart))
                .collect(Collectors.toList());
    }

    /**
     * Following method should be implemented and
     * then used in beforeClass. Here should go all data
     * initialization logic for whole class.
     */
    protected abstract void generateData() throws CradleStorageException, IOException;

    protected void startUp(boolean generateBookPages) throws IOException, CradleStorageException {
        this.session = CassandraCradleHelper.getInstance().getSession();
        this.storage = CassandraCradleHelper.getInstance().getStorage();

        if (generateBookPages) {
            setUpBooksAndPages(bookId, pagesToAdd, pageIdToRemove, dataStart);
        }
    }

    private void setUpBooksAndPages(BookId bookId, List<PageToAdd> pagesToAdd, List<PageId> pageIdToRemove, Instant dataStart) throws CradleStorageException, IOException {
        storage.addBook(new BookToAdd(bookId.getName(), dataStart));

        BookInfo book = storage.addPages(bookId, pagesToAdd);
        for (PageId pageId : pageIdToRemove) {
            storage.removePage(pageId);
        }
        allPages = new ArrayList<>(book.getPages());
        activePages = extractPagesAfterStart(allPages, dataStart);
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

    @NotNull
    protected static Map<PageId, PageInfo> toMap(Collection<PageInfo> result) {
        return result.stream()
                .collect(Collectors.toUnmodifiableMap(
                        PageInfo::getId,
                        Function.identity()
                ));
    }
}
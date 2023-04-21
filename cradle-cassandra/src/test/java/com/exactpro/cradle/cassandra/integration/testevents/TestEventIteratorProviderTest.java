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
package com.exactpro.cradle.cassandra.integration.testevents;

import com.exactpro.cradle.BookInfo;
import com.exactpro.cradle.PageId;
import com.exactpro.cradle.cassandra.dao.testevents.TestEventIteratorProvider;
import com.exactpro.cradle.cassandra.integration.BaseCradleCassandraTest;
import com.exactpro.cradle.cassandra.integration.CassandraCradleHelper;
import com.exactpro.cradle.cassandra.EventBatchDurationWorker;
import com.exactpro.cradle.cassandra.dao.CassandraDataMapper;
import com.exactpro.cradle.cassandra.dao.CassandraDataMapperBuilder;
import com.exactpro.cradle.cassandra.dao.CassandraOperators;
import com.exactpro.cradle.cassandra.resultset.CassandraCradleResultSet;
import com.exactpro.cradle.cassandra.retries.SelectQueryExecutor;
import com.exactpro.cradle.filters.FilterForGreater;
import com.exactpro.cradle.testevents.*;
import com.exactpro.cradle.utils.CradleStorageException;
import org.assertj.core.api.Assertions;
import org.assertj.core.util.Lists;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TestEventIteratorProviderTest extends BaseCradleCassandraTest {

    private static final Logger logger = LoggerFactory.getLogger(TestEventIteratorProviderTest.class);

    private static final String CONTENT = "default_content";
    private static final String FIRST_SCOPE = "test_scope_first";
    private static final String SECOND_SCOPE = "test_scope_second";
    private final static String EVENT_NAME = "default_event_name";
    private final long EVENT_BATCH_DURATION = 24000L;
    private final long EVENTS_IN_BATCH = 4;

    private List<TestEventToStore> data;
    private Map<String, List<StoredTestEvent>> storedData;
    private CassandraOperators operators;
    private ExecutorService composingService = Executors.newSingleThreadExecutor();
    private EventBatchDurationWorker eventBatchDurationWorker;

    @BeforeClass
    public void startUp () throws IOException, InterruptedException, CradleStorageException {
        super.startUp(true);

        setUpOperators ();
        generateData();
        mockEventBatchDurationWorker();
    }

    private void mockEventBatchDurationWorker () {
        eventBatchDurationWorker = Mockito.mock(EventBatchDurationWorker.class);
        Mockito.when(eventBatchDurationWorker.
                getMaxDuration(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.eq(storage.getReadAttrs())))
                .thenReturn(Duration.of(5, ChronoUnit.MINUTES).toMillis());
    }

    @Override
    protected void generateData() throws CradleStorageException, IOException {
        try {
            TestEventToStore b1 = generateTestEvent(FIRST_SCOPE, dataStart.plus(5, ChronoUnit.MINUTES), EVENT_BATCH_DURATION, EVENT_BATCH_DURATION/EVENTS_IN_BATCH);
            TestEventToStore b2 = generateTestEvent(SECOND_SCOPE, dataStart.plus(14, ChronoUnit.MINUTES), EVENT_BATCH_DURATION, EVENT_BATCH_DURATION/EVENTS_IN_BATCH);
            TestEventToStore b3 = generateTestEvent(FIRST_SCOPE, dataStart.plus(21, ChronoUnit.MINUTES), EVENT_BATCH_DURATION, EVENT_BATCH_DURATION/EVENTS_IN_BATCH);
            TestEventToStore b4 = generateTestEvent(SECOND_SCOPE, dataStart.plus(33, ChronoUnit.MINUTES), EVENT_BATCH_DURATION, EVENT_BATCH_DURATION/EVENTS_IN_BATCH);
            TestEventToStore b5 = generateTestEvent(FIRST_SCOPE, dataStart.plus(22, ChronoUnit.MINUTES), EVENT_BATCH_DURATION, EVENT_BATCH_DURATION/EVENTS_IN_BATCH);

            data = List.of(b1, b2, b3, b4, b5);
            storedData = new HashMap<>();

            for (TestEventToStore eventToStore : data) {
                storage.storeTestEvent(eventToStore);
            }

            BookInfo bookInfo = storage.refreshBook(bookId.getName());
            for (TestEventToStore eventToStore : data) {
                PageId pageId = bookInfo.findPage(eventToStore.getStartTimestamp()).getId();
                StoredTestEvent storedTestEvent;

                if (eventToStore.isBatch()) {
                    storedTestEvent = new StoredTestEventBatch(eventToStore.asBatch(), pageId);
                } else {
                    storedTestEvent = new StoredTestEventSingle(eventToStore.asSingle(), pageId);
                }


                storedData.computeIfAbsent(eventToStore.getScope(), e -> new ArrayList<>())
                        .add(storedTestEvent);
            }
        } catch (CradleStorageException | IOException e) {
            logger.error("Error while generating data:", e);
            throw e;
        }
    }

    /*
        Generates test event filled with events
        each of event having `eventDuration` duration
        while batch itself having `batchDuration` duration
     */
    private TestEventToStore generateTestEvent (String scope, Instant start, long batchDuration, long eventDuration) throws CradleStorageException {
        StoredTestEventId parentId = new StoredTestEventId(bookId, scope, start, UUID.randomUUID().toString());
        StoredTestEventId id = new StoredTestEventId(bookId, scope, start, UUID.randomUUID().toString());
        TestEventBatchToStore batch = new TestEventBatchToStoreBuilder(100*1024)
                .name(EVENT_NAME)
                .id(id)
                .parentId(parentId)
                .build();

        for (long i = 0; i < batchDuration; i += eventDuration) {
            batch.addTestEvent(new TestEventSingleToStoreBuilder()
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

    private TestEventIteratorProvider createIteratorProvider(TestEventFilter filter, Instant actualFrom) throws CradleStorageException {
        try {
            return new TestEventIteratorProvider(
                    "",
                    filter,
                    operators,
                    storage.refreshBook(bookId.getName()),
                    composingService,
                    new SelectQueryExecutor(session, composingService, null, null),
                    eventBatchDurationWorker,
                    storage.getReadAttrs(),
                    actualFrom);
        } catch (CradleStorageException e) {
            logger.error(e.getMessage(), e);
            throw e;
        }
    }

    private void setUpOperators() throws IOException, InterruptedException {
        CassandraDataMapper dataMapper = new CassandraDataMapperBuilder(session).build();
        operators = new CassandraOperators(dataMapper, CassandraCradleHelper.getInstance().getStorageSettings());
    }

    @Test(description = "Gets all TestEvents with specific scope")
    public void getAllTestEventsTest () throws CradleStorageException, ExecutionException, InterruptedException {
        try {
            TestEventFilter filter =  new TestEventFilter(bookId, FIRST_SCOPE);
            TestEventIteratorProvider iteratorProvider = createIteratorProvider(filter, dataStart);

            CompletableFuture<CassandraCradleResultSet<StoredTestEvent>> rsFuture = iteratorProvider.nextIterator()
                    .thenApplyAsync(r -> new CassandraCradleResultSet<>(r, iteratorProvider), composingService);

            Iterable<StoredTestEvent> actual = Lists.newArrayList(rsFuture.get().asIterable());
            List<StoredTestEvent> expected = storedData.get(FIRST_SCOPE);

            Assertions.assertThat(actual)
                    .isEqualTo(expected);
        } catch (InterruptedException | ExecutionException | CradleStorageException e) {
            logger.error(e.getMessage(), e);
            throw e;
        }
    }

    @Test(description = "Gets all TestEvents from page with specific scope")
    public void getTestEventsFromPage() throws CradleStorageException, ExecutionException, InterruptedException {
        try {
            TestEventFilter filter =  new TestEventFilter(bookId, FIRST_SCOPE, pages.get(2).getId());
            TestEventIteratorProvider iteratorProvider = createIteratorProvider(filter, dataStart);

            CompletableFuture<CassandraCradleResultSet<StoredTestEvent>> rsFuture = iteratorProvider.nextIterator()
                    .thenApplyAsync(r -> new CassandraCradleResultSet<>(r, iteratorProvider), composingService);

            Iterable<StoredTestEvent> actual = Lists.newArrayList(rsFuture.get().asIterable());
            List<StoredTestEvent> expected = storedData.get(FIRST_SCOPE).subList(1, 3);

            Assertions.assertThat(actual)
                    .isEqualTo(expected);
        } catch (InterruptedException | ExecutionException | CradleStorageException e) {
            logger.error(e.getMessage(), e);
            throw e;
        }
    }

    @Test(description = "Gets TestEvents from empty page")
    public void getTestEventsFromEmptyPage() throws CradleStorageException, ExecutionException, InterruptedException {
        try {
            TestEventFilter filter =  new TestEventFilter(bookId, FIRST_SCOPE, pages.get(4).getId());
            TestEventIteratorProvider iteratorProvider = createIteratorProvider(filter, dataStart);

            CompletableFuture<CassandraCradleResultSet<StoredTestEvent>> rsFuture = iteratorProvider.nextIterator()
                    .thenApplyAsync(r -> new CassandraCradleResultSet<>(r, iteratorProvider), composingService);

            Iterable<StoredTestEvent> actual = Lists.newArrayList(rsFuture.get().asIterable());
            List<StoredTestEvent> expected = Collections.emptyList();

            Assertions.assertThat(actual)
                    .isEqualTo(expected);
        } catch (InterruptedException | ExecutionException | CradleStorageException e) {
            logger.error(e.getMessage(), e);
            throw e;
        }
    }

    @Test(description = "Gets TestEvents from timestamp")
    public void getTestEventsFromTimestamp() throws CradleStorageException, ExecutionException, InterruptedException {
        try {
            TestEventFilter filter =  new TestEventFilter(bookId, SECOND_SCOPE);
            filter.setStartTimestampFrom(FilterForGreater.forGreater(pages.get(2).getStarted()));
            TestEventIteratorProvider iteratorProvider = createIteratorProvider(filter, dataStart);

            CompletableFuture<CassandraCradleResultSet<StoredTestEvent>> rsFuture = iteratorProvider.nextIterator()
                    .thenApplyAsync(r -> new CassandraCradleResultSet<>(r, iteratorProvider), composingService);

            Iterable<StoredTestEvent> actual = Lists.newArrayList(rsFuture.get().asIterable());
            List<StoredTestEvent> expected = storedData.get(SECOND_SCOPE).subList(1, 2);

            Assertions.assertThat(actual)
                    .isEqualTo(expected);
        } catch (InterruptedException | ExecutionException | CradleStorageException e) {
            logger.error(e.getMessage(), e);
            throw e;
        }
    }

    @Test(description = "Gets TestEvents from timestamp which is inside one of the batches, fetching that batch as well")
    public void getTestEventsFromTimestampInsideBatch() throws CradleStorageException, ExecutionException, InterruptedException {
        try {
            TestEventFilter filter =  new TestEventFilter(bookId, FIRST_SCOPE);
            filter.setStartTimestampFrom(FilterForGreater.forGreaterOrEquals(
                    storedData.get(FIRST_SCOPE).get(0).getStartTimestamp().plusMillis(EVENT_BATCH_DURATION)));
            TestEventIteratorProvider iteratorProvider = createIteratorProvider(filter,
                    storedData.get(FIRST_SCOPE).get(0).getStartTimestamp());

            CompletableFuture<CassandraCradleResultSet<StoredTestEvent>> rsFuture = iteratorProvider.nextIterator()
                    .thenApplyAsync(r -> new CassandraCradleResultSet<>(r, iteratorProvider), composingService);

            Iterable<StoredTestEvent> actual = Lists.newArrayList(rsFuture.get().asIterable());
            List<StoredTestEvent> expected = storedData.get(FIRST_SCOPE);

            Assertions.assertThat(actual)
                    .isEqualTo(expected);
        } catch (InterruptedException | ExecutionException | CradleStorageException e) {
            logger.error(e.getMessage(), e);
            throw e;
        }
    }

    @Test(description = "Gets TestEvents from timestamp which is inside one of the batches, fetching that batch as well")
    public void getTestEventsFromTimestampInsideBatch2() throws CradleStorageException, ExecutionException, InterruptedException {
        try {
            TestEventFilter filter =  new TestEventFilter(bookId, FIRST_SCOPE);
            filter.setStartTimestampFrom(FilterForGreater.forGreaterOrEquals(
                    storedData.get(FIRST_SCOPE).get(2).getStartTimestamp()));
            TestEventIteratorProvider iteratorProvider = createIteratorProvider(filter,
                    storedData.get(FIRST_SCOPE).get(2).getStartTimestamp());

            CompletableFuture<CassandraCradleResultSet<StoredTestEvent>> rsFuture = iteratorProvider.nextIterator()
                    .thenApplyAsync(r -> new CassandraCradleResultSet<>(r, iteratorProvider), composingService);

            Iterable<StoredTestEvent> actual = Lists.newArrayList(rsFuture.get().asIterable());
            List<StoredTestEvent> expected = storedData.get(FIRST_SCOPE).subList(2, 3);

            Assertions.assertThat(actual)
                    .isEqualTo(expected);
        } catch (InterruptedException | ExecutionException | CradleStorageException e) {
            logger.error(e.getMessage(), e);
            throw e;
        }
    }
}

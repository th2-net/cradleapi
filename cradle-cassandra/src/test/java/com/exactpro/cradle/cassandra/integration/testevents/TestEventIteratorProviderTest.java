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
package com.exactpro.cradle.cassandra.integration.testevents;

import com.exactpro.cradle.BookInfo;
import com.exactpro.cradle.PageId;
import com.exactpro.cradle.cassandra.EventBatchDurationWorker;
import com.exactpro.cradle.cassandra.dao.CassandraDataMapper;
import com.exactpro.cradle.cassandra.dao.CassandraDataMapperBuilder;
import com.exactpro.cradle.cassandra.dao.CassandraOperators;
import com.exactpro.cradle.cassandra.dao.testevents.TestEventIteratorProvider;
import com.exactpro.cradle.cassandra.integration.BaseCradleCassandraTest;
import com.exactpro.cradle.cassandra.integration.CassandraCradleHelper;
import com.exactpro.cradle.cassandra.resultset.CassandraCradleResultSet;
import com.exactpro.cradle.cassandra.retries.PageSizeAdjustingPolicy;
import com.exactpro.cradle.cassandra.retries.SelectQueryExecutor;
import com.exactpro.cradle.filters.FilterForGreater;
import com.exactpro.cradle.testevents.StoredTestEvent;
import com.exactpro.cradle.testevents.StoredTestEventBatch;
import com.exactpro.cradle.testevents.StoredTestEventSingle;
import com.exactpro.cradle.testevents.TestEventFilter;
import com.exactpro.cradle.testevents.TestEventToStore;
import com.exactpro.cradle.utils.CradleStorageException;
import org.assertj.core.api.Assertions;
import org.assertj.core.util.Lists;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TestEventIteratorProviderTest extends BaseCradleCassandraTest {

    private static final Logger logger = LoggerFactory.getLogger(TestEventIteratorProviderTest.class);
    private static final String FIRST_SCOPE = "test_scope_first";
    private static final String SECOND_SCOPE = "test_scope_second";

    private static final String THIRD_SCOPE = "test_scope_third";

    private final long EVENT_BATCH_DURATION = 24000L;
    private final long EVENTS_IN_BATCH = 4;
    private final List<TestEventToStore> data = new ArrayList<>();
    private Map<String, List<StoredTestEvent>> storedData;
    private CassandraOperators operators;
    private final ExecutorService composingService = Executors.newFixedThreadPool(3);
    private EventBatchDurationWorker eventBatchDurationWorker;

    @BeforeClass
    public void startUp() throws IOException, InterruptedException, CradleStorageException {
        super.startUp(true);

        setUpOperators();
        generateData();
        mockEventBatchDurationWorker();
    }

    private void mockEventBatchDurationWorker() {
        eventBatchDurationWorker = Mockito.mock(EventBatchDurationWorker.class);
        Mockito.when(eventBatchDurationWorker.
                        getMaxDuration(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.eq(storage.getReadAttrs())))
                .thenReturn(EVENT_BATCH_DURATION);
    }

    @Override
    protected void generateData() throws CradleStorageException, IOException {
        try {
            TestEventToStore b1 = generateTestEvent(FIRST_SCOPE, dataStart.plus(5, ChronoUnit.MINUTES), EVENT_BATCH_DURATION, EVENT_BATCH_DURATION / EVENTS_IN_BATCH);
            TestEventToStore b2 = generateTestEvent(SECOND_SCOPE, dataStart.plus(14, ChronoUnit.MINUTES), EVENT_BATCH_DURATION, EVENT_BATCH_DURATION / EVENTS_IN_BATCH);
            TestEventToStore b3 = generateTestEvent(FIRST_SCOPE, dataStart.plus(21, ChronoUnit.MINUTES), EVENT_BATCH_DURATION, EVENT_BATCH_DURATION / EVENTS_IN_BATCH);
            TestEventToStore b4 = generateTestEvent(SECOND_SCOPE, dataStart.plus(33, ChronoUnit.MINUTES), EVENT_BATCH_DURATION, EVENT_BATCH_DURATION / EVENTS_IN_BATCH);


            data.addAll(List.of(b1, b2, b3, b4));
            data.addAll(generateOverlappingEvents());

            storedData = new HashMap<>();

            for (TestEventToStore eventToStore : data) {
                storage.storeTestEvent(eventToStore);
            }

            BookInfo bookInfo = storage.refreshBook(bookId.getName());
            for (TestEventToStore eventToStore : data) {
                PageId pageId = bookInfo.findPage(eventToStore.getStartTimestamp()).getId();
                StoredTestEvent storedTestEvent;

                if (eventToStore.isBatch()) {
                    // FIXME: correct test
//                    storedTestEvent = new StoredTestEventBatch(eventToStore.asBatch(), pageId);
                } else {
                    storedTestEvent = new StoredTestEventSingle(eventToStore.asSingle(), pageId);
                }

                    // FIXME: correct test
//                storedData.computeIfAbsent(eventToStore.getScope(), e -> new ArrayList<>())
//                        .add(storedTestEvent);
            }
        } catch (CradleStorageException | IOException e) {
            logger.error("Error while generating data:", e);
            throw e;
        }
    }

    private List<TestEventToStore> generateOverlappingEvents() throws CradleStorageException {
        Instant baseStartTime = dataStart.plus(35, ChronoUnit.MINUTES);

        long b1Duration = EVENT_BATCH_DURATION;
        long b2Duration = (long) (b1Duration * 0.1);
        long b3Duration = (long) (b1Duration * 0.7);
        long b4Duration = (long) (b1Duration * 0.3);
        long b5Duration = (long) (b1Duration * 0.4);

        long shift = (long) (EVENT_BATCH_DURATION * 0.05);

        TestEventToStore b1 = generateTestEvent(THIRD_SCOPE, baseStartTime, EVENT_BATCH_DURATION, b1Duration / EVENTS_IN_BATCH);
        TestEventToStore b2 = generateTestEvent(THIRD_SCOPE, baseStartTime.plus(Duration.of(shift, ChronoUnit.MILLIS)), b2Duration, b2Duration / EVENTS_IN_BATCH);
        TestEventToStore b3 = generateTestEvent(THIRD_SCOPE, baseStartTime.plus(Duration.of(shift + b2Duration, ChronoUnit.MILLIS)), b3Duration, b3Duration / EVENTS_IN_BATCH);
        TestEventToStore b4 = generateTestEvent(THIRD_SCOPE, baseStartTime.plus(Duration.of(2 * shift + b2Duration, ChronoUnit.MILLIS)), b4Duration, b4Duration / EVENTS_IN_BATCH);
        TestEventToStore b5 = generateTestEvent(THIRD_SCOPE, baseStartTime.plus(Duration.of(3 * shift + b2Duration, ChronoUnit.MILLIS)), b5Duration, b5Duration / EVENTS_IN_BATCH);

        //We place b2 the last because it is one that will be excluded in expected result, and it's easier to do sublist if this element is the last one.
        return List.of(b1, b3, b4, b5, b2);
    }

    private TestEventIteratorProvider createIteratorProvider(TestEventFilter filter, Instant actualFrom) throws CradleStorageException {
        try {
            return new TestEventIteratorProvider(
                    "",
                    filter,
                    operators,
                    storage.refreshBook(bookId.getName()),
                    composingService,
                    new SelectQueryExecutor(session, composingService, new PageSizeAdjustingPolicy(1, 2), null),
                    eventBatchDurationWorker,
                    storage.getReadAttrs(),
                    actualFrom);
        } catch (CradleStorageException e) {
            logger.error(e.getMessage(), e);
            throw e;
        }
    }

    private void setUpOperators() {
        CassandraDataMapper dataMapper = new CassandraDataMapperBuilder(session).build();
        operators = new CassandraOperators(dataMapper, CassandraCradleHelper.getInstance().getStorageSettings());
    }

    @Test(description = "Gets all TestEvents with specific scope")
    public void getAllTestEventsTest() throws CradleStorageException, ExecutionException, InterruptedException {
        try {
            TestEventFilter filter = new TestEventFilter(bookId, FIRST_SCOPE);
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
            TestEventFilter filter = new TestEventFilter(bookId, FIRST_SCOPE, pages.get(2).getId());
            TestEventIteratorProvider iteratorProvider = createIteratorProvider(filter, dataStart);

            CompletableFuture<CassandraCradleResultSet<StoredTestEvent>> rsFuture = iteratorProvider.nextIterator()
                    .thenApplyAsync(r -> new CassandraCradleResultSet<>(r, iteratorProvider), composingService);

            Iterable<StoredTestEvent> actual = Lists.newArrayList(rsFuture.get().asIterable());
            List<StoredTestEvent> expected = storedData.get(FIRST_SCOPE).subList(1, 2);

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
            TestEventFilter filter = new TestEventFilter(bookId, FIRST_SCOPE, pages.get(4).getId());
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
            TestEventFilter filter = new TestEventFilter(bookId, SECOND_SCOPE);
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
            TestEventFilter filter = new TestEventFilter(bookId, FIRST_SCOPE);
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


    @Test(description = "Gets TestEvents from timestamp which will include several batches among them invalid ones too (because of query shift). check that invalid batches are removed by filtering iterator. ")
    public void testFilteringIteratorProvider() throws CradleStorageException, ExecutionException, InterruptedException {
        try {
            TestEventFilter filter = new TestEventFilter(bookId, THIRD_SCOPE);
            //storedData.get(THIRD_SCOPE).get(3) will get b5 (from overlapping events) start time
            filter.setStartTimestampFrom(FilterForGreater.forGreaterOrEquals(
                    storedData.get(THIRD_SCOPE).get(3).getStartTimestamp()));
            TestEventIteratorProvider iteratorProvider = createIteratorProvider(filter,
                    storedData.get(THIRD_SCOPE).get(3).getStartTimestamp());

            CompletableFuture<CassandraCradleResultSet<StoredTestEvent>> rsFuture = iteratorProvider.nextIterator()
                    .thenApplyAsync(r -> new CassandraCradleResultSet<>(r, iteratorProvider), composingService);

            Iterable<StoredTestEvent> actual = Lists.newArrayList(rsFuture.get().asIterable());
            //this sublist will exclude b2 (from overlapping events)
            List<StoredTestEvent> expected = storedData.get(THIRD_SCOPE).subList(0, 4);

            Assertions.assertThat(actual)
                    .isEqualTo(expected);
        } catch (InterruptedException | ExecutionException | CradleStorageException e) {
            logger.error(e.getMessage(), e);
            throw e;
        }
    }
}

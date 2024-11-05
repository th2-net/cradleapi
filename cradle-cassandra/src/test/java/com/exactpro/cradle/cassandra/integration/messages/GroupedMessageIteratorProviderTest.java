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

package com.exactpro.cradle.cassandra.integration.messages;

import com.exactpro.cradle.CoreStorageSettings;
import com.exactpro.cradle.Direction;
import com.exactpro.cradle.Order;
import com.exactpro.cradle.cassandra.dao.messages.GroupedMessageIteratorProvider;
import com.exactpro.cradle.cassandra.integration.BaseCradleCassandraTest;
import com.exactpro.cradle.cassandra.integration.CassandraCradleHelper;
import com.exactpro.cradle.cassandra.dao.CassandraDataMapper;
import com.exactpro.cradle.cassandra.dao.CassandraDataMapperBuilder;
import com.exactpro.cradle.cassandra.dao.CassandraOperators;
import com.exactpro.cradle.cassandra.resultset.CassandraCradleResultSet;
import com.exactpro.cradle.cassandra.retries.SelectQueryExecutor;
import com.exactpro.cradle.filters.FilterForGreater;
import com.exactpro.cradle.messages.GroupedMessageBatchToStore;
import com.exactpro.cradle.messages.GroupedMessageFilter;
import com.exactpro.cradle.messages.StoredGroupedMessageBatch;
import com.exactpro.cradle.utils.CradleStorageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

public class GroupedMessageIteratorProviderTest extends BaseCradleCassandraTest {

    private static final Logger logger = LoggerFactory.getLogger(GroupedMessageIteratorProviderTest.class);
    private static final String GROUP_NAME = "test_group";
    private static final String FIRST_SESSION_ALIAS = "test_session_alias_first";
    private static final String SECOND_SESSION_ALIAS = "test_session_alias_second";

    private final long storeActionRejectionThreshold = new CoreStorageSettings().calculateStoreActionRejectionThreshold();

    private List<StoredGroupedMessageBatch> storedData;
    private CassandraOperators operators;
    private final ExecutorService composingService = Executors.newFixedThreadPool(3);

    @BeforeClass
    public void startUp () throws IOException, InterruptedException, CradleStorageException {
        super.startUp(true);

        setUpOperators();
        generateData();
    }

    private void setUpOperators() {
        CassandraDataMapper dataMapper = new CassandraDataMapperBuilder(session).build();
        operators = new CassandraOperators(dataMapper, CassandraCradleHelper.getInstance().getStorageSettings());
    }

    @Override
    protected void generateData () {
        try {
            GroupedMessageBatchToStore b1 =  new GroupedMessageBatchToStore(GROUP_NAME, 1024, storeActionRejectionThreshold);
            b1.addMessage(generateMessage(FIRST_SESSION_ALIAS, Direction.FIRST, 5, 1L));
            b1.addMessage(generateMessage(SECOND_SESSION_ALIAS, Direction.SECOND, 9, 2L));

            GroupedMessageBatchToStore b2 =  new GroupedMessageBatchToStore(GROUP_NAME, 1024, storeActionRejectionThreshold);
            b2.addMessage(generateMessage(FIRST_SESSION_ALIAS, Direction.FIRST, 15, 3L));
            b2.addMessage(generateMessage(SECOND_SESSION_ALIAS, Direction.SECOND, 19, 4L));

            GroupedMessageBatchToStore b3 =  new GroupedMessageBatchToStore(GROUP_NAME, 1024, storeActionRejectionThreshold);
            b3.addMessage(generateMessage(FIRST_SESSION_ALIAS, Direction.FIRST, 25, 5L));
            b3.addMessage(generateMessage(SECOND_SESSION_ALIAS, Direction.SECOND, 25, 6L));

            List<GroupedMessageBatchToStore> data = List.of(b1, b2, b3);
            storedData = List.of(
                    MessageTestUtils.groupedMessageBatchToStored(activePages.get(0).getId(), null, b1),
                    MessageTestUtils.groupedMessageBatchToStored(activePages.get(1).getId(), null, b2),
                    MessageTestUtils.groupedMessageBatchToStored(activePages.get(2).getId(), null, b3));

            for (var el : data) {
                storage.storeGroupedMessageBatch(el);
            }
        } catch (CradleStorageException | IOException e) {
            logger.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    private GroupedMessageIteratorProvider createIteratorProvider(GroupedMessageFilter groupedMessageFilter) throws CradleStorageException {
        try {
            return new GroupedMessageIteratorProvider(
                    "",
                    groupedMessageFilter,
                    operators,
                    storage.refreshBook(bookId.getName()),
                    composingService,
                    new SelectQueryExecutor(session, composingService, null, null),
                    storage.getReadAttrs(),
                    Order.DIRECT);
        } catch (CradleStorageException e) {
            logger.error(e.getMessage(), e);
            throw e;
        }
    }

    @Test(description = "Simply gets all grouped messages from iterator provider")
    public void getAllGroupedMessagesTest () throws ExecutionException, InterruptedException, CradleStorageException {
        GroupedMessageFilter groupedMessageFilter =  new GroupedMessageFilter(bookId, GROUP_NAME);
        GroupedMessageIteratorProvider iteratorProvider = createIteratorProvider(groupedMessageFilter);

        CompletableFuture<CassandraCradleResultSet<StoredGroupedMessageBatch>> rsFuture = iteratorProvider.nextIterator()
                .thenApplyAsync(r -> new CassandraCradleResultSet<>(r, iteratorProvider), composingService);

        try {
            Iterable<StoredGroupedMessageBatch> actual =  rsFuture.get().asIterable();
            List<StoredGroupedMessageBatch> expected = storedData;

            assertThat(actual)
                    .usingRecursiveFieldByFieldElementComparatorIgnoringFields("recDate")
                    .isEqualTo(expected);
        } catch (InterruptedException | ExecutionException e) {
            logger.error(e.getMessage(), e);
            throw e;
        }
    }

    @Test(description = "Gets first 2 grouped messages from iterator provider")
    public void getFirstTwoGroupedMessagesTest () throws ExecutionException, InterruptedException, CradleStorageException {
        GroupedMessageFilter groupedMessageFilter = new GroupedMessageFilter(bookId, GROUP_NAME);
        groupedMessageFilter.setLimit(2);
        GroupedMessageIteratorProvider iteratorProvider = createIteratorProvider(groupedMessageFilter);

        CompletableFuture<CassandraCradleResultSet<StoredGroupedMessageBatch>> rsFuture = iteratorProvider.nextIterator()
                .thenApplyAsync(r -> new CassandraCradleResultSet<>(r, iteratorProvider), composingService);

        try {
            Iterable<StoredGroupedMessageBatch> actual =  rsFuture.get().asIterable();
            List<StoredGroupedMessageBatch> expected = storedData.subList(0, 2);

            assertThat(actual)
                    .usingRecursiveFieldByFieldElementComparatorIgnoringFields("recDate")
                    .isEqualTo(expected);
        } catch (InterruptedException | ExecutionException e) {
            logger.error(e.getMessage(), e);
            throw e;
        }
    }

    @Test(description = "Gets grouped messages from iterator provider starting with second page")
    public void getGroupedMessagesAfterSecondPageTest () throws ExecutionException, InterruptedException, CradleStorageException {
        GroupedMessageFilter groupedMessageFilter = new GroupedMessageFilter(bookId, GROUP_NAME);
        groupedMessageFilter.setFrom(FilterForGreater.forGreaterOrEquals(activePages.get(1).getStarted()));
        GroupedMessageIteratorProvider iteratorProvider = createIteratorProvider(groupedMessageFilter);

        CompletableFuture<CassandraCradleResultSet<StoredGroupedMessageBatch>> rsFuture = iteratorProvider.nextIterator()
                .thenApplyAsync(r -> new CassandraCradleResultSet<>(r, iteratorProvider), composingService);

        try {
            Iterable<StoredGroupedMessageBatch> actual =  rsFuture.get().asIterable();
            List<StoredGroupedMessageBatch> expected = storedData.subList(1, 3);

            assertThat(actual)
                    .usingRecursiveFieldByFieldElementComparatorIgnoringFields("recDate")
                    .isEqualTo(expected);
        } catch (InterruptedException | ExecutionException e) {
            logger.error(e.getMessage(), e);
            throw e;
        }
    }

    @Test(description = "Tries to get grouped messages by filter which has negative limit, should end with exception")
    public void tryToGetGroupedMessagesWithNegativeLimitTest () {
        Throwable throwable = catchThrowable(() -> new GroupedMessageFilter(bookId, GROUP_NAME).setLimit(-1));
        assertThat(throwable).hasMessage("Invalid limit value: -1. limit must be greater than 0");
    }

    @Test(description = "Gets grouped messages from iterator provider starting with second page and limit 1")
    public void getGroupedMessagesAfterSecondPageWithLimitTest () throws ExecutionException, InterruptedException, CradleStorageException {
        GroupedMessageFilter groupedMessageFilter = new GroupedMessageFilter(bookId, GROUP_NAME);
        groupedMessageFilter.setFrom(FilterForGreater.forGreaterOrEquals(activePages.get(1).getStarted()));
        groupedMessageFilter.setLimit(1);
        GroupedMessageIteratorProvider iteratorProvider = createIteratorProvider(groupedMessageFilter);

        CompletableFuture<CassandraCradleResultSet<StoredGroupedMessageBatch>> rsFuture = iteratorProvider.nextIterator()
                .thenApplyAsync(r -> new CassandraCradleResultSet<>(r, iteratorProvider), composingService);

        try {
            Iterable<StoredGroupedMessageBatch> actual =  rsFuture.get().asIterable();
            List<StoredGroupedMessageBatch> expected = storedData.subList(1, 2);

            assertThat(actual)
                    .usingRecursiveFieldByFieldElementComparatorIgnoringFields("recDate")
                    .isEqualTo(expected);
        } catch (InterruptedException | ExecutionException e) {
            logger.error(e.getMessage(), e);
            throw e;
        }
    }

    @Test(description = "Gets grouped messages from second page")
    public void getGroupedMessagesFromSecondPage () throws ExecutionException, InterruptedException, CradleStorageException {
        GroupedMessageFilter groupedMessageFilter = new GroupedMessageFilter(bookId, GROUP_NAME);
        groupedMessageFilter.setFrom(FilterForGreater.forGreaterOrEquals(activePages.get(1).getStarted()));
        groupedMessageFilter.setLimit(1);
        GroupedMessageIteratorProvider iteratorProvider = createIteratorProvider(groupedMessageFilter);

        CompletableFuture<CassandraCradleResultSet<StoredGroupedMessageBatch>> rsFuture = iteratorProvider.nextIterator()
                .thenApplyAsync(r -> new CassandraCradleResultSet<>(r, iteratorProvider), composingService);

        try {
            Iterable<StoredGroupedMessageBatch> actual =  rsFuture.get().asIterable();
            List<StoredGroupedMessageBatch> expected = storedData.subList(1, 2);

            assertThat(actual)
                    .usingRecursiveFieldByFieldElementComparatorIgnoringFields("recDate")
                    .isEqualTo(expected);
        } catch (InterruptedException | ExecutionException e) {
            logger.error(e.getMessage(), e);
            throw e;
        }
    }

    @Test(description = "Gets grouped messages from empty page")
    public void getGroupedMessagesFromEmptyPage () throws ExecutionException, InterruptedException, CradleStorageException {
        GroupedMessageFilter groupedMessageFilter = new GroupedMessageFilter(bookId, GROUP_NAME);
        groupedMessageFilter.setFrom(FilterForGreater.forGreaterOrEquals(activePages.get(3).getStarted()));
        groupedMessageFilter.setLimit(1);
        GroupedMessageIteratorProvider iteratorProvider = createIteratorProvider(groupedMessageFilter);

        CompletableFuture<CassandraCradleResultSet<StoredGroupedMessageBatch>> rsFuture = iteratorProvider.nextIterator()
                .thenApplyAsync(r -> new CassandraCradleResultSet<>(r, iteratorProvider), composingService);

        try {
            Iterable<StoredGroupedMessageBatch> actual =  rsFuture.get().asIterable();
            List<StoredGroupedMessageBatch> expected = Collections.emptyList();

            assertThat(actual)
                    .usingRecursiveFieldByFieldElementComparatorIgnoringFields("recDate")
                    .isEqualTo(expected);
        } catch (InterruptedException | ExecutionException e) {
            logger.error(e.getMessage(), e);
            throw e;
        }
    }
}

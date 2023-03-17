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

package com.exactpro.cradle.cassandra.dao.messages;

import com.datastax.oss.driver.api.core.MappedAsyncPagingIterable;
import com.exactpro.cradle.*;
import com.exactpro.cradle.cassandra.BaseCradleCassandraTest;
import com.exactpro.cradle.cassandra.CassandraCradleHelper;
import com.exactpro.cradle.cassandra.dao.CassandraDataMapper;
import com.exactpro.cradle.cassandra.dao.CassandraDataMapperBuilder;
import com.exactpro.cradle.cassandra.dao.CassandraOperators;
import com.exactpro.cradle.cassandra.resultset.CassandraCradleResultSet;
import com.exactpro.cradle.cassandra.retries.SelectQueryExecutor;
import com.exactpro.cradle.filters.FilterForGreater;
import com.exactpro.cradle.messages.*;
import com.exactpro.cradle.utils.CradleStorageException;
import org.assertj.core.api.Assertions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class GroupedMessageIteratorProviderTest extends BaseCradleCassandraTest {

    private static final Logger logger = LoggerFactory.getLogger(GroupedMessageIteratorProviderTest.class);

    public static String content = "default_content";
    private static final String GROUP_NAME = "test_group_GroupedMessageIteratorProviderTest";
    private static final String FIRST_SESSION_ALIAS = "test_session_alias_first_GroupedMessageIteratorProviderTest";
    private static final String SECOND_SESSION_ALIAS = "test_session_alias_second_GroupedMessageIteratorProviderTest";

    private List<GroupedMessageBatchToStore> data;
    private List<StoredGroupedMessageBatch> storedData;
    private CompletableFuture<MappedAsyncPagingIterable<GroupedMessageBatchEntity>> iterable;
    private CassandraOperators operators;
    private ExecutorService composingService = Executors.newSingleThreadExecutor();
    public final static String protocol = "default_message_protocol";
    @BeforeClass
    public void startUp () {
        super.startUp(true);

        setUpOperators ();
        generateData();
    }

    private void setUpOperators() {
        CassandraDataMapper dataMapper = new CassandraDataMapperBuilder(session).build();
        operators = new CassandraOperators(dataMapper, CassandraCradleHelper.getInstance().getStorageSettings());
    }

    private void generateData () {
        try {
            GroupedMessageBatchToStore b1 =  new GroupedMessageBatchToStore(GROUP_NAME, 1024);
            b1.addMessage(MessageToStore.builder()
                    .bookId(bookId)
                    .sessionAlias(FIRST_SESSION_ALIAS)
                    .direction(Direction.FIRST)
                    .timestamp(dataStart.plus(5, ChronoUnit.MINUTES))
                    .sequence(1L)
                    .content(content.getBytes(StandardCharsets.UTF_8))
                    .protocol(protocol)
                    .build());
            b1.addMessage(MessageToStore.builder()
                    .bookId(bookId)
                    .sessionAlias(SECOND_SESSION_ALIAS)
                    .direction(Direction.SECOND)
                    .timestamp(dataStart.plus(9, ChronoUnit.MINUTES))
                    .sequence(2L)
                    .content(content.getBytes(StandardCharsets.UTF_8))
                    .protocol(protocol)
                    .build());

            GroupedMessageBatchToStore b2 =  new GroupedMessageBatchToStore(GROUP_NAME, 1024);
            b2.addMessage(MessageToStore.builder()
                    .bookId(bookId)
                    .sessionAlias(FIRST_SESSION_ALIAS)
                    .direction(Direction.FIRST)
                    .timestamp(dataStart.plus(15, ChronoUnit.MINUTES))
                    .sequence(3L)
                    .content(content.getBytes(StandardCharsets.UTF_8))
                    .protocol(protocol)
                    .build());
            b2.addMessage(MessageToStore.builder()
                    .bookId(bookId)
                    .sessionAlias(SECOND_SESSION_ALIAS)
                    .direction(Direction.SECOND)
                    .timestamp(dataStart.plus(19, ChronoUnit.MINUTES))
                    .sequence(4L)
                    .content(content.getBytes(StandardCharsets.UTF_8))
                    .protocol(protocol)
                    .build());

            GroupedMessageBatchToStore b3 =  new GroupedMessageBatchToStore(GROUP_NAME, 1024);
            b3.addMessage(MessageToStore.builder()
                    .bookId(bookId)
                    .sessionAlias(FIRST_SESSION_ALIAS)
                    .direction(Direction.FIRST)
                    .timestamp(dataStart.plus(25, ChronoUnit.MINUTES))
                    .sequence(5L)
                    .content(content.getBytes(StandardCharsets.UTF_8))
                    .protocol(protocol)
                    .build());
            b3.addMessage(MessageToStore.builder()
                    .bookId(bookId)
                    .sessionAlias(SECOND_SESSION_ALIAS)
                    .direction(Direction.SECOND)
                    .timestamp(dataStart.plus(29, ChronoUnit.MINUTES))
                    .sequence(6L)
                    .content(content.getBytes(StandardCharsets.UTF_8))
                    .protocol(protocol)
                    .build());

            data = List.of(b1, b2, b3);
            storedData = List.of(
                    MessageTestUtils.groupedMessageBatchToStored(pages.get(0).getId(), null, b1),
                    MessageTestUtils.groupedMessageBatchToStored(pages.get(1).getId(), null, b2),
                    MessageTestUtils.groupedMessageBatchToStored(pages.get(2).getId(), null, b3));

            for (var el : data) {
                storage.storeGroupedMessageBatch(el);
            }
        } catch (CradleStorageException | IOException e) {
            logger.error("", e);
            throw new RuntimeException(e);
        }
    }

    private GroupedMessageIteratorProvider createIteratorProvider(GroupedMessageFilter groupedMessageFilter) {
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
            logger.error("", e);
            throw new RuntimeException(e);
        }
    }

    @Test(description = "Simply gets all grouped messages from iterator provider")
    public void getAllGroupedMessagesTest () {
        GroupedMessageFilter groupedMessageFilter =  new GroupedMessageFilter(bookId, GROUP_NAME);
        GroupedMessageIteratorProvider iteratorProvider = createIteratorProvider(groupedMessageFilter);

        CompletableFuture<CassandraCradleResultSet<StoredGroupedMessageBatch>> rsFuture = iteratorProvider.nextIterator()
                .thenApplyAsync(r -> new CassandraCradleResultSet<>(r, iteratorProvider), composingService);

        try {
            Iterable<StoredGroupedMessageBatch> actual =  rsFuture.get().asIterable();
            List<StoredGroupedMessageBatch> expected = storedData;

            Assertions.assertThat(actual)
                    .usingElementComparatorIgnoringFields("recDate")
                    .isEqualTo(expected);
        } catch (InterruptedException | ExecutionException e) {
            logger.error("", e);
            Assertions.fail(e.getMessage());
        }
    }

    @Test(description = "Gets first 2 grouped messages from iterator provider")
    public void getFirstTwoGroupedMessagesTest () {
        GroupedMessageFilter groupedMessageFilter = new GroupedMessageFilter(bookId, GROUP_NAME);
        groupedMessageFilter.setLimit(2);
        GroupedMessageIteratorProvider iteratorProvider = createIteratorProvider(groupedMessageFilter);

        CompletableFuture<CassandraCradleResultSet<StoredGroupedMessageBatch>> rsFuture = iteratorProvider.nextIterator()
                .thenApplyAsync(r -> new CassandraCradleResultSet<>(r, iteratorProvider), composingService);

        try {
            Iterable<StoredGroupedMessageBatch> actual =  rsFuture.get().asIterable();
            List<StoredGroupedMessageBatch> expected = storedData.subList(0, 2);

            Assertions.assertThat(actual)
                    .usingElementComparatorIgnoringFields("recDate")
                    .isEqualTo(expected);
        } catch (InterruptedException | ExecutionException e) {
            logger.error("", e);
            Assertions.fail(e.getMessage());
        }
    }

    @Test(description = "Gets grouped messages from iterator provider starting with second page")
    public void getGroupedMessagesAfterSecondPageTest () {
        GroupedMessageFilter groupedMessageFilter = new GroupedMessageFilter(bookId, GROUP_NAME);
        groupedMessageFilter.setFrom(FilterForGreater.forGreaterOrEquals(pages.get(1).getStarted()));
        GroupedMessageIteratorProvider iteratorProvider = createIteratorProvider(groupedMessageFilter);

        CompletableFuture<CassandraCradleResultSet<StoredGroupedMessageBatch>> rsFuture = iteratorProvider.nextIterator()
                .thenApplyAsync(r -> new CassandraCradleResultSet<>(r, iteratorProvider), composingService);

        try {
            Iterable<StoredGroupedMessageBatch> actual =  rsFuture.get().asIterable();
            List<StoredGroupedMessageBatch> expected = storedData.subList(1, 3);

            Assertions.assertThat(actual)
                    .usingElementComparatorIgnoringFields("recDate")
                    .isEqualTo(expected);
        } catch (InterruptedException | ExecutionException e) {
            logger.error("", e);
            Assertions.fail(e.getMessage());
        }
    }

    @Test(description = "Tries to get grouped messages by filter which has negative limit, should end with exception")
    public void tryToGetGroupedMessagesWithNegativeLimitTest () {

        try {
            GroupedMessageFilter groupedMessageFilter = new GroupedMessageFilter(bookId, GROUP_NAME);
            groupedMessageFilter.setLimit(-1);
            GroupedMessageIteratorProvider iteratorProvider = createIteratorProvider(groupedMessageFilter);

            CompletableFuture<CassandraCradleResultSet<StoredGroupedMessageBatch>> rsFuture = iteratorProvider.nextIterator()
                    .thenApplyAsync(r -> new CassandraCradleResultSet<>(r, iteratorProvider), composingService);

            Iterable<StoredGroupedMessageBatch> actual =  rsFuture.get().asIterable();

            Assertions.fail("Exception wasn't thrown while getting messages with negative limit");
        } catch (Exception e) {
            // Test passed
        }
    }

    @Test(description = "Gets grouped messages from iterator provider starting with second page and limit 1")
    public void getGroupedMessagesAfterSecondPageWithLimitTest () {
        GroupedMessageFilter groupedMessageFilter = new GroupedMessageFilter(bookId, GROUP_NAME);
        groupedMessageFilter.setFrom(FilterForGreater.forGreaterOrEquals(pages.get(1).getStarted()));
        groupedMessageFilter.setLimit(1);
        GroupedMessageIteratorProvider iteratorProvider = createIteratorProvider(groupedMessageFilter);

        CompletableFuture<CassandraCradleResultSet<StoredGroupedMessageBatch>> rsFuture = iteratorProvider.nextIterator()
                .thenApplyAsync(r -> new CassandraCradleResultSet<>(r, iteratorProvider), composingService);

        try {
            Iterable<StoredGroupedMessageBatch> actual =  rsFuture.get().asIterable();
            List<StoredGroupedMessageBatch> expected = storedData.subList(1, 2);

            Assertions.assertThat(actual)
                    .usingElementComparatorIgnoringFields("recDate")
                    .isEqualTo(expected);
        } catch (InterruptedException | ExecutionException e) {
            logger.error("", e);
            Assertions.fail(e.getMessage());
        }
    }

    @Test(description = "Gets grouped messages from second page")
    public void getGroupedMessagesFromSecondPage () {
        GroupedMessageFilter groupedMessageFilter = new GroupedMessageFilter(bookId, GROUP_NAME);
        groupedMessageFilter.setFrom(FilterForGreater.forGreaterOrEquals(pages.get(1).getStarted()));
        groupedMessageFilter.setLimit(1);
        GroupedMessageIteratorProvider iteratorProvider = createIteratorProvider(groupedMessageFilter);

        CompletableFuture<CassandraCradleResultSet<StoredGroupedMessageBatch>> rsFuture = iteratorProvider.nextIterator()
                .thenApplyAsync(r -> new CassandraCradleResultSet<>(r, iteratorProvider), composingService);

        try {
            Iterable<StoredGroupedMessageBatch> actual =  rsFuture.get().asIterable();
            List<StoredGroupedMessageBatch> expected = storedData.subList(1, 2);

            Assertions.assertThat(actual)
                    .usingElementComparatorIgnoringFields("recDate")
                    .isEqualTo(expected);
        } catch (InterruptedException | ExecutionException e) {
            logger.error("", e);
            Assertions.fail(e.getMessage());
        }
    }

    @Test(description = "Gets grouped messages from empty page")
    public void getGroupedMessagesFromEmptyPage () {
        GroupedMessageFilter groupedMessageFilter = new GroupedMessageFilter(bookId, GROUP_NAME);
        groupedMessageFilter.setFrom(FilterForGreater.forGreaterOrEquals(pages.get(3).getStarted()));
        groupedMessageFilter.setLimit(1);
        GroupedMessageIteratorProvider iteratorProvider = createIteratorProvider(groupedMessageFilter);

        CompletableFuture<CassandraCradleResultSet<StoredGroupedMessageBatch>> rsFuture = iteratorProvider.nextIterator()
                .thenApplyAsync(r -> new CassandraCradleResultSet<>(r, iteratorProvider), composingService);

        try {
            Iterable<StoredGroupedMessageBatch> actual =  rsFuture.get().asIterable();
            List<StoredGroupedMessageBatch> expected = Collections.emptyList();

            Assertions.assertThat(actual)
                    .usingElementComparatorIgnoringFields("recDate")
                    .isEqualTo(expected);
        } catch (InterruptedException | ExecutionException e) {
            logger.error("", e);
            Assertions.fail(e.getMessage());
        }
    }
}

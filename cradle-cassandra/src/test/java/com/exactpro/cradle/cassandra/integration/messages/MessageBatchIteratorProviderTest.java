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
package com.exactpro.cradle.cassandra.integration.messages;

import com.exactpro.cradle.BookInfo;
import com.exactpro.cradle.Direction;
import com.exactpro.cradle.cassandra.dao.messages.MessageBatchesIteratorProvider;
import com.exactpro.cradle.cassandra.integration.BaseCradleCassandraTest;
import com.exactpro.cradle.cassandra.integration.CassandraCradleHelper;
import com.exactpro.cradle.cassandra.dao.CassandraDataMapper;
import com.exactpro.cradle.cassandra.dao.CassandraDataMapperBuilder;
import com.exactpro.cradle.cassandra.dao.CassandraOperators;
import com.exactpro.cradle.cassandra.resultset.CassandraCradleResultSet;
import com.exactpro.cradle.cassandra.retries.SelectQueryExecutor;
import com.exactpro.cradle.messages.*;
import com.exactpro.cradle.utils.CradleStorageException;
import org.assertj.core.api.Assertions;
import org.assertj.core.util.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MessageBatchIteratorProviderTest extends BaseCradleCassandraTest {

    private static final Logger logger = LoggerFactory.getLogger(MessageBatchIteratorProviderTest.class);
    private static final String GROUP_NAME = "test_group";
    private static final String FIRST_SESSION_ALIAS = "test_session_alias";
    private static final String SECOND_SESSION_ALIAS = "test_session_alias";

    private List<GroupedMessageBatchToStore> data;
    private Map<MessageBatchIteratorProviderTest.StoredMessageKey, List<StoredMessageBatch>> storedData;
    private CassandraOperators operators;
    private ExecutorService composingService = Executors.newSingleThreadExecutor();

    @BeforeClass
    public void startUp () throws IOException, InterruptedException, CradleStorageException {
        super.startUp(true);

        setUpOperators ();
        generateData();
    }

    private static class StoredMessageKey {
        private final String sessionAlias;
        private final Direction direction;

        public StoredMessageKey (StoredMessage message) {
            this.sessionAlias = message.getSessionAlias();
            this.direction = message.getDirection();
        }

        public StoredMessageKey (String sessionAlias, Direction direction) {
            this.sessionAlias = sessionAlias;
            this.direction = direction;
        }

        public String getSessionAlias() {
            return sessionAlias;
        }

        public Direction getDirection() {
            return direction;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof MessageBatchIteratorProviderTest.StoredMessageKey)) return false;
            MessageBatchIteratorProviderTest.StoredMessageKey that = (MessageBatchIteratorProviderTest.StoredMessageKey) o;
            return Objects.equals(getSessionAlias(), that.getSessionAlias()) && getDirection() == that.getDirection();
        }

        @Override
        public int hashCode() {
            return Objects.hash(getSessionAlias(), getDirection());
        }
    }

    private void setUpOperators() throws IOException, InterruptedException {
        CassandraDataMapper dataMapper = new CassandraDataMapperBuilder(session).build();
        operators = new CassandraOperators(dataMapper, CassandraCradleHelper.getInstance().getStorageSettings());
    }

    @Override
    protected void generateData () {
        /*
         Storing grouped messages results
         in storing usual messages as well
         */
        try {
            GroupedMessageBatchToStore b1 =  new GroupedMessageBatchToStore(GROUP_NAME, 1024);
            b1.addMessage(generateMessage(FIRST_SESSION_ALIAS, Direction.FIRST, 5, 1L));
            b1.addMessage(generateMessage(FIRST_SESSION_ALIAS, Direction.FIRST, 6, 2L));
            b1.addMessage(generateMessage(SECOND_SESSION_ALIAS, Direction.SECOND, 7, 3L));
            b1.addMessage(generateMessage(SECOND_SESSION_ALIAS, Direction.SECOND, 8, 4L));

            GroupedMessageBatchToStore b2 =  new GroupedMessageBatchToStore(GROUP_NAME, 1024);
            b2.addMessage(generateMessage(FIRST_SESSION_ALIAS, Direction.FIRST, 12, 5L));
            b2.addMessage(generateMessage(SECOND_SESSION_ALIAS, Direction.SECOND, 13, 6L));
            b2.addMessage(generateMessage(FIRST_SESSION_ALIAS, Direction.FIRST, 14, 7L));
            b2.addMessage(generateMessage(SECOND_SESSION_ALIAS, Direction.SECOND, 15, 8L));

            GroupedMessageBatchToStore b3 =  new GroupedMessageBatchToStore(GROUP_NAME, 1024);
            b3.addMessage(generateMessage(FIRST_SESSION_ALIAS, Direction.FIRST, 25, 9L));
            b3.addMessage(generateMessage(SECOND_SESSION_ALIAS, Direction.SECOND, 26, 10L));
            b3.addMessage(generateMessage(FIRST_SESSION_ALIAS, Direction.FIRST, 27, 11L));
            b3.addMessage(generateMessage(SECOND_SESSION_ALIAS, Direction.SECOND, 28, 12L));

            data = List.of(b1, b2, b3);
            storedData = new HashMap<>();
            BookInfo bookInfo = storage.refreshBook(bookId.getName());
            for (GroupedMessageBatchToStore groupedBatch : data) {
                Collection<MessageBatchToStore> batchesToStore = groupedBatch.getSessionMessageBatches();
                for (MessageBatchToStore batch : batchesToStore) {
                    MessageBatchIteratorProviderTest.StoredMessageKey key = new MessageBatchIteratorProviderTest.StoredMessageKey(
                            batch.getSessionAlias(),
                            batch.getDirection());

                    StoredMessageBatch storedMessageBatch = MessageTestUtils.messageBatchToStored(bookInfo.findPage(batch.getFirstTimestamp()).getId(), null, batch);

                    storedData.computeIfAbsent(key, e -> new ArrayList<>())
                            .add(storedMessageBatch);
                }
            }

            for (GroupedMessageBatchToStore el : data) {
                storage.storeGroupedMessageBatch(el);
            }
        } catch (CradleStorageException | IOException e) {
            logger.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    private MessageBatchesIteratorProvider createIteratorProvider(MessageFilter messageFilter) throws CradleStorageException {
        try {
            return new MessageBatchesIteratorProvider(
                    "",
                    messageFilter,
                    operators,
                    storage.refreshBook(bookId.getName()),
                    composingService,
                    new SelectQueryExecutor(session, composingService, null, null),
                    storage.getReadAttrs());
        } catch (CradleStorageException e) {
            logger.error(e.getMessage(), e);
            throw e;
        }
    }

    @Test(description = "Gets all messages by session_alias and direction")
    public void getAllMessagesFromSessionTest() throws CradleStorageException, ExecutionException, InterruptedException {
        try {
            MessageFilter messageFilter =  new MessageFilter(bookId, FIRST_SESSION_ALIAS, Direction.FIRST);
            MessageBatchesIteratorProvider iteratorProvider = createIteratorProvider(messageFilter);

            CompletableFuture<CassandraCradleResultSet<StoredMessageBatch>> rsFuture = iteratorProvider.nextIterator()
                    .thenApplyAsync(r -> new CassandraCradleResultSet<>(r, iteratorProvider), composingService);

            Iterable<StoredMessageBatch> actual = Lists.newArrayList(rsFuture.get().asIterable());
            List<StoredMessageBatch> expected = storedData.get(new MessageBatchIteratorProviderTest.StoredMessageKey(FIRST_SESSION_ALIAS, Direction.FIRST));

            Assertions.assertThat(actual)
                    .usingElementComparatorIgnoringFields("recDate")
                    .isEqualTo(expected);
        } catch (InterruptedException | ExecutionException | CradleStorageException e) {
            logger.error(e.getMessage(), e);
            throw e;
        }
    }

    @Test(description = "Gets messages from page by session_alias and direction")
    public void getMessagesFromSessionAndPage() throws CradleStorageException, ExecutionException, InterruptedException {
        try {
            MessageFilter messageFilter =  new MessageFilter(bookId, SECOND_SESSION_ALIAS, Direction.SECOND, pages.get(1).getId());
            MessageBatchesIteratorProvider iteratorProvider = createIteratorProvider(messageFilter);

            CompletableFuture<CassandraCradleResultSet<StoredMessageBatch>> rsFuture = iteratorProvider.nextIterator()
                    .thenApplyAsync(r -> new CassandraCradleResultSet<>(r, iteratorProvider), composingService);

            Iterable<StoredMessageBatch> actual = Lists.newArrayList(rsFuture.get().asIterable());
            List<StoredMessageBatch> expected = storedData.get(new MessageBatchIteratorProviderTest.StoredMessageKey(SECOND_SESSION_ALIAS, Direction.SECOND)).subList(1,2);

            Assertions.assertThat(actual)
                    .usingElementComparatorIgnoringFields("recDate")
                    .isEqualTo(expected);
        } catch (InterruptedException | ExecutionException | CradleStorageException e) {
            logger.error(e.getMessage(), e);
            throw e;
        }
    }

    @Test(description = "Gets messages from page by session_alias and direction")
    public void getMessagesFromEmptyPage() throws CradleStorageException, ExecutionException, InterruptedException {
        try {
            MessageFilter messageFilter =  new MessageFilter(bookId, FIRST_SESSION_ALIAS, Direction.FIRST, pages.get(4).getId());
            MessageBatchesIteratorProvider iteratorProvider = createIteratorProvider(messageFilter);

            CompletableFuture<CassandraCradleResultSet<StoredMessageBatch>> rsFuture = iteratorProvider.nextIterator()
                    .thenApplyAsync(r -> new CassandraCradleResultSet<>(r, iteratorProvider), composingService);

            Iterable<StoredMessageBatch> actual = Lists.newArrayList(rsFuture.get().asIterable());
            List<StoredMessageBatch> expected = Collections.emptyList();

            Assertions.assertThat(actual)
                    .usingElementComparatorIgnoringFields("recDate")
                    .isEqualTo(expected);
        } catch (InterruptedException | ExecutionException | CradleStorageException e) {
            logger.error(e.getMessage(), e);
            throw e;
        }
    }

    @Test(description = "tries to get messages with negative limit, provider should throw exception")
    public void tryToMessageBatchWithNegativeLimit() {
        try {
            MessageFilter messageFilter =  new MessageFilter(bookId, FIRST_SESSION_ALIAS, Direction.FIRST);
            messageFilter.setLimit(-1);
            MessageBatchesIteratorProvider iteratorProvider = createIteratorProvider(messageFilter);

            CompletableFuture<CassandraCradleResultSet<StoredMessageBatch>> rsFuture = iteratorProvider.nextIterator()
                    .thenApplyAsync(r -> new CassandraCradleResultSet<>(r, iteratorProvider), composingService);

            rsFuture.get().asIterable();

            Assertions.fail("Exception wasn't thrown while getting messages with negative limit");
        } catch (Exception e) {
            // Test passed
        }
    }
}

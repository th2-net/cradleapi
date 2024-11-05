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

import com.exactpro.cradle.*;
import com.exactpro.cradle.cassandra.dao.messages.MessagesIteratorProvider;
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

public class MessageIteratorProviderTest extends BaseCradleCassandraTest {

    private static final Logger logger = LoggerFactory.getLogger(MessageIteratorProviderTest.class);
    private static final String GROUP_NAME = "test_group";
    private static final String FIRST_SESSION_ALIAS = "test_session_alias";
    private static final String SECOND_SESSION_ALIAS = "test_session_alias";

    private final long storeActionRejectionThreshold = new CoreStorageSettings().calculateStoreActionRejectionThreshold();

    private Map<StoredMessageKey, List<StoredMessage>> storedData;
    private CassandraOperators operators;
    private final ExecutorService composingService = Executors.newFixedThreadPool(3);

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
            if (!(o instanceof StoredMessageKey)) return false;
            StoredMessageKey that = (StoredMessageKey) o;
            return Objects.equals(getSessionAlias(), that.getSessionAlias()) && getDirection() == that.getDirection();
        }

        @Override
        public int hashCode() {
            return Objects.hash(getSessionAlias(), getDirection());
        }
    }

    private void setUpOperators() {
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
            storedData = new HashMap<>();
            BookInfo bookInfo = storage.refreshBook(bookId.getName());
            for (GroupedMessageBatchToStore batch : data) {
                for (StoredMessage message : batch.getMessages()) {
                    StoredMessageKey key = new StoredMessageKey(message);

                    storedData.computeIfAbsent(key, e -> new ArrayList<>())
                            .add(MessageTestUtils.messageToStored(message, bookInfo.findPage(message.getTimestamp()).getId()));
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

    private MessagesIteratorProvider createIteratorProvider(MessageFilter messageFilter) throws CradleStorageException {
        try {
            return new MessagesIteratorProvider(
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

    @Test(description = "Simply gets all messages by session_alias and direction iterator provider")
    public void getAllGroupedMessagesTest () throws CradleStorageException, ExecutionException, InterruptedException {
        try {
            MessageFilter messageFilter =  new MessageFilter(bookId, FIRST_SESSION_ALIAS, Direction.FIRST);
            MessagesIteratorProvider iteratorProvider = createIteratorProvider(messageFilter);

            CompletableFuture<CassandraCradleResultSet<StoredMessage>> rsFuture = iteratorProvider.nextIterator()
                    .thenApplyAsync(r -> new CassandraCradleResultSet<>(r, iteratorProvider), composingService);

            Iterable<StoredMessage> actual = Lists.newArrayList(rsFuture.get().asIterable());
            List<StoredMessage> expected = storedData.get(new StoredMessageKey(FIRST_SESSION_ALIAS, Direction.FIRST));

            Assertions.assertThat(actual)
                    .isEqualTo(expected);
        } catch (InterruptedException | ExecutionException | CradleStorageException e) {
            logger.error(e.getMessage(), e);
            throw e;
        }
    }

    @Test(description = "Gets messages second page")
    public void getGroupedMessagesFromSecondPage () throws CradleStorageException, ExecutionException, InterruptedException {
        try {
            MessageFilter messageFilter =  new MessageFilter(bookId, SECOND_SESSION_ALIAS, Direction.SECOND, activePages.get(1).getId());
            MessagesIteratorProvider iteratorProvider = createIteratorProvider(messageFilter);

            CompletableFuture<CassandraCradleResultSet<StoredMessage>> rsFuture = iteratorProvider.nextIterator()
                    .thenApplyAsync(r -> new CassandraCradleResultSet<>(r, iteratorProvider), composingService);

            Iterable<StoredMessage> actual = Lists.newArrayList(rsFuture.get().asIterable());
            List<StoredMessage> expected = storedData.get(new StoredMessageKey(SECOND_SESSION_ALIAS, Direction.SECOND)).subList(1, 2);

            Assertions.assertThat(actual)
                    .isEqualTo(expected);
        } catch (InterruptedException | ExecutionException | CradleStorageException e) {
            logger.error(e.getMessage(), e);
            throw e;
        }
    }

    @Test(description = "Gets messages from empty page")
    public void getGroupedMessagesFromEmptyPageTest () throws CradleStorageException, ExecutionException, InterruptedException {
        try {
            MessageFilter messageFilter =  new MessageFilter(bookId, FIRST_SESSION_ALIAS, Direction.FIRST, activePages.get(3).getId());
            MessagesIteratorProvider iteratorProvider = createIteratorProvider(messageFilter);

            CompletableFuture<CassandraCradleResultSet<StoredMessage>> rsFuture = iteratorProvider.nextIterator()
                    .thenApplyAsync(r -> new CassandraCradleResultSet<>(r, iteratorProvider), composingService);

            Iterable<StoredMessage> actual = Lists.newArrayList(rsFuture.get().asIterable());
            List<StoredMessage> expected = Collections.emptyList();

            Assertions.assertThat(actual)
                    .isEqualTo(expected);
        } catch (InterruptedException | ExecutionException | CradleStorageException e) {
            logger.error(e.getMessage(), e);
            throw e;
        }
    }

    @Test(description = "tries to get messages with negative limit, provider should throw exception")
    public void tryToMessagesWithNegativeLimit() {
        try {
            MessageFilter messageFilter =  new MessageFilter(bookId, FIRST_SESSION_ALIAS, Direction.FIRST);
            messageFilter.setLimit(-1);
            MessagesIteratorProvider iteratorProvider = createIteratorProvider(messageFilter);

            CompletableFuture<CassandraCradleResultSet<StoredMessage>> rsFuture = iteratorProvider.nextIterator()
                    .thenApplyAsync(r -> new CassandraCradleResultSet<>(r, iteratorProvider), composingService);

            rsFuture.get().asIterable();

            Assertions.fail("Exception wasn't thrown while getting messages with negative limit");
        } catch (Exception e) {
            // Test passed
        }
    }
}

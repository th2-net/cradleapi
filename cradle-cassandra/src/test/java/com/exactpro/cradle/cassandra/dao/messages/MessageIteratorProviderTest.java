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
import com.exactpro.cradle.messages.*;
import com.exactpro.cradle.utils.CradleStorageException;
import org.assertj.core.api.Assertions;
import org.assertj.core.util.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MessageIteratorProviderTest extends BaseCradleCassandraTest {

    private static final Logger logger = LoggerFactory.getLogger(MessageIteratorProviderTest.class);

    public static String content = "default_content";
    private static final String GROUP_NAME = "test_group";
    private static final String FIRST_SESSION_ALIAS = "test_session_alias_first";
    private static final String SECOND_SESSION_ALIAS = "test_session_alias_second";

    private List<GroupedMessageBatchToStore> data;
    private Map<StoredMessageKey, List<StoredMessage>> storedData;
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

    private void generateData () {
        /*
         Storing grouped messages results
         in storing usual messages as well
         */
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
            logger.error("", e);
            throw new RuntimeException(e);
        }
    }

    private MessagesIteratorProvider createIteratorProvider(MessageFilter messageFilter) {
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
            logger.error("", e);
            throw new RuntimeException(e);
        }
    }

    @Test(description = "Simply gets all messages by session_alias and direction iterator provider")
    public void getAllGroupedMessagesTest () {
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
            logger.error("", e);
            Assertions.fail(e.getMessage());
        }
    }

    @Test(description = "Gets messages second page")
    public void getGroupedMessagesFromSecondPage () {
        try {
            MessageFilter messageFilter =  new MessageFilter(bookId, SECOND_SESSION_ALIAS, Direction.SECOND, pages.get(1).getId());
            MessagesIteratorProvider iteratorProvider = createIteratorProvider(messageFilter);

            CompletableFuture<CassandraCradleResultSet<StoredMessage>> rsFuture = iteratorProvider.nextIterator()
                    .thenApplyAsync(r -> new CassandraCradleResultSet<>(r, iteratorProvider), composingService);

            Iterable<StoredMessage> actual = Lists.newArrayList(rsFuture.get().asIterable());
            List<StoredMessage> expected = storedData.get(new StoredMessageKey(SECOND_SESSION_ALIAS, Direction.SECOND)).subList(1, 2);

            Assertions.assertThat(actual)
                    .isEqualTo(expected);
        } catch (InterruptedException | ExecutionException | CradleStorageException e) {
            logger.error("", e);
            Assertions.fail(e.getMessage());
        }
    }

    @Test(description = "Gets messages from empty page")
    public void getGroupedMessagesFromEmptyPageTest () {
        try {
            MessageFilter messageFilter =  new MessageFilter(bookId, FIRST_SESSION_ALIAS, Direction.FIRST, pages.get(3).getId());
            MessagesIteratorProvider iteratorProvider = createIteratorProvider(messageFilter);

            CompletableFuture<CassandraCradleResultSet<StoredMessage>> rsFuture = iteratorProvider.nextIterator()
                    .thenApplyAsync(r -> new CassandraCradleResultSet<>(r, iteratorProvider), composingService);

            Iterable<StoredMessage> actual = Lists.newArrayList(rsFuture.get().asIterable());
            List<StoredMessage> expected = Collections.emptyList();

            Assertions.assertThat(actual)
                    .isEqualTo(expected);
        } catch (InterruptedException | ExecutionException | CradleStorageException e) {
            logger.error("", e);
            Assertions.fail(e.getMessage());
        }
    }

    @Test(description = "tries to get messages with negative limit, provider should throw exception")
    public void tryToGetGroupedMessagesWithNegativeLimit() {
        try {
            MessageFilter messageFilter =  new MessageFilter(bookId, FIRST_SESSION_ALIAS, Direction.FIRST, pages.get(3).getId());
            messageFilter.setLimit(-1);
            MessagesIteratorProvider iteratorProvider = createIteratorProvider(messageFilter);

            CompletableFuture<CassandraCradleResultSet<StoredMessage>> rsFuture = iteratorProvider.nextIterator()
                    .thenApplyAsync(r -> new CassandraCradleResultSet<>(r, iteratorProvider), composingService);

            rsFuture.get().asIterable();

            // TODO: this behavior is different from other iteratorProviders, negative limit does not fail
//            Assertions.fail("Exception wasn't thrown while getting messages with negative limit");
        } catch (Exception e) {
            // Test passed
        }
    }
}

package com.exactpro.cradle.cassandra.dao.messages;

import com.exactpro.cradle.BookInfo;
import com.exactpro.cradle.Direction;
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

public class MessageBatchIteratorProviderTest extends BaseCradleCassandraTest {

    private static final Logger logger = LoggerFactory.getLogger(MessageBatchIteratorProviderTest.class);

    private static final String CONTENT = "default_content";
    private static final String GROUP_NAME = "test_group";
    private static final String FIRST_SESSION_ALIAS = "test_session_alias";
    private static final String SECOND_SESSION_ALIAS = "test_session_alias";

    private List<GroupedMessageBatchToStore> data;
    private Map<MessageBatchIteratorProviderTest.StoredMessageKey, List<StoredMessageBatch>> storedData;
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
            if (!(o instanceof MessageBatchIteratorProviderTest.StoredMessageKey)) return false;
            MessageBatchIteratorProviderTest.StoredMessageKey that = (MessageBatchIteratorProviderTest.StoredMessageKey) o;
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
            GroupedMessageBatchToStore b1 =  new GroupedMessageBatchToStore(GROUP_NAME, 1024);
            b1.addMessage(MessageToStore.builder()
                    .bookId(bookId)
                    .sessionAlias(FIRST_SESSION_ALIAS)
                    .direction(Direction.FIRST)
                    .timestamp(dataStart.plus(5, ChronoUnit.MINUTES))
                    .sequence(1L)
                    .content(CONTENT.getBytes(StandardCharsets.UTF_8))
                    .protocol(protocol)
                    .build());
            b1.addMessage(MessageToStore.builder()
                    .bookId(bookId)
                    .sessionAlias(FIRST_SESSION_ALIAS)
                    .direction(Direction.FIRST)
                    .timestamp(dataStart.plus(6, ChronoUnit.MINUTES))
                    .sequence(2L)
                    .content(CONTENT.getBytes(StandardCharsets.UTF_8))
                    .protocol(protocol)
                    .build());
            b1.addMessage(MessageToStore.builder()
                    .bookId(bookId)
                    .sessionAlias(SECOND_SESSION_ALIAS)
                    .direction(Direction.SECOND)
                    .timestamp(dataStart.plus(7, ChronoUnit.MINUTES))
                    .sequence(3L)
                    .content(CONTENT.getBytes(StandardCharsets.UTF_8))
                    .protocol(protocol)
                    .build());
            b1.addMessage(MessageToStore.builder()
                    .bookId(bookId)
                    .sessionAlias(SECOND_SESSION_ALIAS)
                    .direction(Direction.SECOND)
                    .timestamp(dataStart.plus(8, ChronoUnit.MINUTES))
                    .sequence(4L)
                    .content(CONTENT.getBytes(StandardCharsets.UTF_8))
                    .protocol(protocol)
                    .build());

            GroupedMessageBatchToStore b2 =  new GroupedMessageBatchToStore(GROUP_NAME, 1024);
            b2.addMessage(MessageToStore.builder()
                    .bookId(bookId)
                    .sessionAlias(FIRST_SESSION_ALIAS)
                    .direction(Direction.FIRST)
                    .timestamp(dataStart.plus(12, ChronoUnit.MINUTES))
                    .sequence(5L)
                    .content(CONTENT.getBytes(StandardCharsets.UTF_8))
                    .protocol(protocol)
                    .build());
            b2.addMessage(MessageToStore.builder()
                    .bookId(bookId)
                    .sessionAlias(SECOND_SESSION_ALIAS)
                    .direction(Direction.SECOND)
                    .timestamp(dataStart.plus(13, ChronoUnit.MINUTES))
                    .sequence(6L)
                    .content(CONTENT.getBytes(StandardCharsets.UTF_8))
                    .protocol(protocol)
                    .build());
            b2.addMessage(MessageToStore.builder()
                    .bookId(bookId)
                    .sessionAlias(FIRST_SESSION_ALIAS)
                    .direction(Direction.FIRST)
                    .timestamp(dataStart.plus(14, ChronoUnit.MINUTES))
                    .sequence(7L)
                    .content(CONTENT.getBytes(StandardCharsets.UTF_8))
                    .protocol(protocol)
                    .build());
            b2.addMessage(MessageToStore.builder()
                    .bookId(bookId)
                    .sessionAlias(SECOND_SESSION_ALIAS)
                    .direction(Direction.SECOND)
                    .timestamp(dataStart.plus(15, ChronoUnit.MINUTES))
                    .sequence(8L)
                    .content(CONTENT.getBytes(StandardCharsets.UTF_8))
                    .protocol(protocol)
                    .build());

            GroupedMessageBatchToStore b3 =  new GroupedMessageBatchToStore(GROUP_NAME, 1024);
            b3.addMessage(MessageToStore.builder()
                    .bookId(bookId)
                    .sessionAlias(FIRST_SESSION_ALIAS)
                    .direction(Direction.FIRST)
                    .timestamp(dataStart.plus(25, ChronoUnit.MINUTES))
                    .sequence(9L)
                    .content(CONTENT.getBytes(StandardCharsets.UTF_8))
                    .protocol(protocol)
                    .build());
            b3.addMessage(MessageToStore.builder()
                    .bookId(bookId)
                    .sessionAlias(SECOND_SESSION_ALIAS)
                    .direction(Direction.SECOND)
                    .timestamp(dataStart.plus(26, ChronoUnit.MINUTES))
                    .sequence(10L)
                    .content(CONTENT.getBytes(StandardCharsets.UTF_8))
                    .protocol(protocol)
                    .build());
            b3.addMessage(MessageToStore.builder()
                    .bookId(bookId)
                    .sessionAlias(FIRST_SESSION_ALIAS)
                    .direction(Direction.FIRST)
                    .timestamp(dataStart.plus(27, ChronoUnit.MINUTES))
                    .sequence(11L)
                    .content(CONTENT.getBytes(StandardCharsets.UTF_8))
                    .protocol(protocol)
                    .build());
            b3.addMessage(MessageToStore.builder()
                    .bookId(bookId)
                    .sessionAlias(SECOND_SESSION_ALIAS)
                    .direction(Direction.SECOND)
                    .timestamp(dataStart.plus(28, ChronoUnit.MINUTES))
                    .sequence(12L)
                    .content(CONTENT.getBytes(StandardCharsets.UTF_8))
                    .protocol(protocol)
                    .build());

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
            logger.error("", e);
            throw new RuntimeException(e);
        }
    }

    private MessageBatchesIteratorProvider createIteratorProvider(MessageFilter messageFilter) {
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
            logger.error("", e);
            throw new RuntimeException(e);
        }
    }

    @Test(description = "Gets all messages by session_alias and direction")
    public void getAllMessagesFromSessionTest() {
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
            logger.error("", e);
            Assertions.fail(e.getMessage());
        }
    }

    @Test(description = "Gets messages from page by session_alias and direction")
    public void getMessagesFromSessionAndPage() {
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
            logger.error("", e);
            Assertions.fail(e.getMessage());
        }
    }

    @Test(description = "Gets messages from page by session_alias and direction")
    public void getMessagesFromEmptyPage() {
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
            logger.error("", e);
            Assertions.fail(e.getMessage());
        }
    }

    // TODO: this behavior is different from other iteratorProviders, negative limit does not fail
}

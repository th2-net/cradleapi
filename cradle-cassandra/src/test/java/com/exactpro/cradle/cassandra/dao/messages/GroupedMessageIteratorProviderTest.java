package com.exactpro.cradle.cassandra.dao.messages;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.MappedAsyncPagingIterable;
import com.exactpro.cradle.*;
import com.exactpro.cradle.cassandra.CassandraCradleManager;
import com.exactpro.cradle.cassandra.CassandraCradleStorage;
import com.exactpro.cradle.cassandra.CassandraStorageSettings;
import com.exactpro.cradle.cassandra.connection.CassandraConnectionSettings;
import com.exactpro.cradle.cassandra.dao.CassandraDataMapper;
import com.exactpro.cradle.cassandra.dao.CassandraDataMapperBuilder;
import com.exactpro.cradle.cassandra.dao.CassandraOperators;
import com.exactpro.cradle.cassandra.resultset.CassandraCradleResultSet;
import com.exactpro.cradle.cassandra.retries.SelectQueryExecutor;
import com.exactpro.cradle.messages.*;
import com.exactpro.cradle.utils.CradleStorageException;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.jupiter.api.Assertions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class GroupedMessageIteratorProviderTest {

    private static final Logger logger = LoggerFactory.getLogger(GroupedMessageIteratorProviderTest.class);


    public static String content = "default_content";
    public static String KEYSPACE_NAME = "test_keyspace";
    public static String LOCAL_DATACENTER_NAME = "datacenter1";
    private static final BookId BOOKID = new BookId("test_book");
    private static final String PAGE_PREFIX = "test_page_";
    private static final String GROUP_NAME = "test_group";
    private static final int DEFAULT_LIMIT = 10;

    private static final Instant END = Instant.now();
    private static final Instant START = END.minus(1, ChronoUnit.HOURS);

    private List<GroupedMessageBatchToStore> data;
    private CompletableFuture<MappedAsyncPagingIterable<GroupedMessageBatchEntity>> iterable;
    private CassandraOperators operators;
    private ExecutorService composingService = Executors.newSingleThreadExecutor();
    public final static String protocol = "default_message_protocol";
    private CqlSession session;
    private CassandraConnectionSettings cassandraConnectionSettings;
    private CassandraCradleStorage storage;
    private CassandraStorageSettings cassandraStorageSettings;
    private List<PageInfo> pages;

    private void setUpEmbeddedCassandra () {
        try {
            EmbeddedCassandraServerHelper.startEmbeddedCassandra();
            session = EmbeddedCassandraServerHelper.getSession();
            cassandraConnectionSettings = new CassandraConnectionSettings(
                    EmbeddedCassandraServerHelper.getHost(),
                    EmbeddedCassandraServerHelper.getNativeTransportPort(),
                    LOCAL_DATACENTER_NAME);
        } catch (IOException | InterruptedException e) {
            logger.info("", e);
            throw new RuntimeException(e);
        }
    }

    private void setUpCradle () {
        try {
            cassandraStorageSettings = new CassandraStorageSettings(
                    5000,
                    ConsistencyLevel.ONE,
                    ConsistencyLevel.ONE);
            cassandraStorageSettings.setResultPageSize(5);
            cassandraStorageSettings.setKeyspace(KEYSPACE_NAME);
            cassandraStorageSettings.setCounterPersistenceInterval(0);

            CassandraCradleManager manager = new CassandraCradleManager(cassandraConnectionSettings, cassandraStorageSettings, true);
            storage = (CassandraCradleStorage) manager.getStorage();
        } catch (CradleStorageException | IOException e) {
            logger.info("", e);
        }
    }

    @BeforeClass
    public void beforeClass () {
        setUpEmbeddedCassandra();
        setUpCradle ();
        setUpOperators ();
        setUpBooksAndPages();
    }

    private void setUpBooksAndPages () {
        try {
            storage.addBook(new BookToAdd(BOOKID.getName(), START));

            pages = List.of(
                    new PageInfo(
                            new PageId(BOOKID, PAGE_PREFIX+0),
                            START,
                            START.plus(10, ChronoUnit.MINUTES), ""),
                    new PageInfo(
                            new PageId(BOOKID, PAGE_PREFIX+1),
                            START.plus(10, ChronoUnit.MINUTES),
                            START.plus(20, ChronoUnit.MINUTES), ""),
                    new PageInfo(
                            new PageId(BOOKID, PAGE_PREFIX+2),
                            START.plus(20, ChronoUnit.MINUTES),
                            START.plus(30, ChronoUnit.MINUTES), ""),
                    new PageInfo(
                            new PageId(BOOKID, PAGE_PREFIX+3),
                            START.plus(30, ChronoUnit.MINUTES),
                            START.plus(40, ChronoUnit.MINUTES), ""),
                    new PageInfo(
                            new PageId(BOOKID, PAGE_PREFIX+4),
                            START.plus(40, ChronoUnit.MINUTES),
                            START.plus(50, ChronoUnit.MINUTES), ""),
                    new PageInfo(
                            new PageId(BOOKID, PAGE_PREFIX+5),
                            START.plus(50, ChronoUnit.MINUTES),
                            START.plus(60, ChronoUnit.MINUTES), ""));

            for (var el : pages) {
                storage.addPage(BOOKID, el.getId().getName(), el.getStarted(), el.getComment());
            }
        } catch (CradleStorageException | IOException e) {
            logger.info("", e);
        }
    }

    @Test
    public void simpleTest () throws CradleStorageException {
        generateData();

        GroupedMessageIteratorProvider iteratorProvider = createIteratorProvider(pages, GROUP_NAME, null);

        var rsFuture = iteratorProvider.nextIterator()
                .thenApplyAsync(r -> new CassandraCradleResultSet<>(r, iteratorProvider), composingService);

        try {
            var resultSet =  rsFuture.get();
            int i = 0;
            for (var batch : resultSet.asIterable()) {
                var converted = MessageTestUtils.groupedMessageBatchToStored(batch.getFirstMessage().getPageId(), batch.getRecDate(), data.get(i));
                Assertions.assertEquals(batch, converted);
                i ++;
            }
        } catch (InterruptedException | ExecutionException e) {
            logger.info("", e);
        }
    }

    private GroupedMessageIteratorProvider createIteratorProvider( Collection<PageInfo> pages, String groupName, String initPage) {
        try {
            GroupedMessageFilter groupMessageFilter;

            if (initPage == null) {
                groupMessageFilter = new GroupedMessageFilter(BOOKID, groupName);
            } else {
                groupMessageFilter = new GroupedMessageFilter(BOOKID, new PageId(BOOKID, initPage), groupName);
            }

            return new GroupedMessageIteratorProvider(
                    "",
                        groupMessageFilter,
                        operators,
                        storage.refreshBook(BOOKID.getName()),
                        composingService,
                        new SelectQueryExecutor(session, composingService, null, null),
                        storage.getReadAttrs(),
                        Order.DIRECT);
        } catch (CradleStorageException e) {
            logger.info("", e);
            throw new RuntimeException(e);
        }
    }

    private void setUpOperators() {
        CassandraDataMapper dataMapper = new CassandraDataMapperBuilder(session).build();
        operators = new CassandraOperators(dataMapper, cassandraStorageSettings);
    }

    private void generateData () throws CradleStorageException {
        var b1 =  new GroupedMessageBatchToStore(GROUP_NAME, 1024);
        b1.addMessage(MessageToStore.builder()
                .bookId(BOOKID)
                .sessionAlias(GROUP_NAME + 1)
                .direction(Direction.FIRST)
                .timestamp(START.plus(5, ChronoUnit.MINUTES))
                .sequence(1L)
                .content(content.getBytes(StandardCharsets.UTF_8))
                .protocol(protocol)
                .build());

        b1.addMessage(MessageToStore.builder()
                .bookId(BOOKID)
                .sessionAlias(GROUP_NAME + 1)
                .direction(Direction.SECOND)
                .timestamp(START.plus(9, ChronoUnit.MINUTES))
                .sequence(2L)
                .content(content.getBytes(StandardCharsets.UTF_8))
                .protocol(protocol)
                .build());
        var b2 =  new GroupedMessageBatchToStore(GROUP_NAME, 1024);
        b2.addMessage(MessageToStore.builder()
                .bookId(BOOKID)
                .sessionAlias(GROUP_NAME + 1)
                .direction(Direction.FIRST)
                .timestamp(START.plus(15, ChronoUnit.MINUTES))
                .sequence(3L)
                .content(content.getBytes(StandardCharsets.UTF_8))
                .protocol(protocol)
                .build());

        b2.addMessage(MessageToStore.builder()
                .bookId(BOOKID)
                .sessionAlias(GROUP_NAME + 1)
                .direction(Direction.SECOND)
                .timestamp(START.plus(19, ChronoUnit.MINUTES))
                .sequence(4L)
                .content(content.getBytes(StandardCharsets.UTF_8))
                .protocol(protocol)
                .build());
        var b3 =  new GroupedMessageBatchToStore(GROUP_NAME, 1024);
        b3.addMessage(MessageToStore.builder()
                .bookId(BOOKID)
                .sessionAlias(GROUP_NAME + 1)
                .direction(Direction.FIRST)
                .timestamp(START.plus(25, ChronoUnit.MINUTES))
                .sequence(5L)
                .content(content.getBytes(StandardCharsets.UTF_8))
                .protocol(protocol)
                .build());

        b3.addMessage(MessageToStore.builder()
                .bookId(BOOKID)
                .sessionAlias(GROUP_NAME + 1)
                .direction(Direction.SECOND)
                .timestamp(START.plus(29, ChronoUnit.MINUTES))
                .sequence(6L)
                .content(content.getBytes(StandardCharsets.UTF_8))
                .protocol(protocol)
                .build());
        data = List.of(b1, b2, b3);

        try {
            for (var el : data) {
                storage.storeGroupedMessageBatch(el);
            }
        } catch (IOException e) {
            logger.info("", e);
        }
    }

}

package com.exactpro.cradle.cassandra.integration.messages;

import com.exactpro.cradle.CoreStorageSettings;
import com.exactpro.cradle.Direction;
import com.exactpro.cradle.cassandra.integration.BaseCradleCassandraTest;
import com.exactpro.cradle.counters.Interval;
import com.exactpro.cradle.messages.GroupedMessageBatchToStore;
import com.exactpro.cradle.utils.CradleStorageException;
import org.assertj.core.api.Assertions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.time.temporal.ChronoUnit;
import java.util.*;

import static org.assertj.core.util.Lists.newArrayList;

public class PageSessionsAndGroupsApiTest extends BaseCradleCassandraTest {
    private static final Logger logger = LoggerFactory.getLogger(PageSessionsAndGroupsApiTest.class);

    private static final String GROUP1_NAME = "test_group1";

    private static final String GROUP2_NAME = "test_group2";

    private static final String GROUP3_NAME = "test_group3";

    private static final String GROUP4_NAME = "test_group4";

    private static final String SESSION_ALIAS1 = "test_session_alias1";

    private static final String SESSION_ALIAS2 = "test_session_alias2";

    private static final String SESSION_ALIAS3 = "test_session_alias3";

    private static final String SESSION_ALIAS4 = "test_session_alias4";

    private static final String SESSION_ALIAS5 = "test_session_alias5";

    private static final String SESSION_ALIAS6 = "test_session_alias6";

    private static final Set<String> allSessions = Set.of(
            SESSION_ALIAS1,
            SESSION_ALIAS2,
            SESSION_ALIAS3,
            SESSION_ALIAS4,
            SESSION_ALIAS5,
            SESSION_ALIAS6
    );

    private static final Set<String> allGroups = Set.of(
            GROUP1_NAME,
            GROUP2_NAME,
            GROUP3_NAME,
            GROUP4_NAME
    );

    private final long storeActionRejectionThreshold = new CoreStorageSettings().calculateStoreActionRejectionThreshold();


    @BeforeClass
    public void startUp() throws IOException, InterruptedException, CradleStorageException {
        super.startUp(true);
        generateData();
    }

    @Override
    protected void generateData() {
        try {
            GroupedMessageBatchToStore b1 = new GroupedMessageBatchToStore(GROUP1_NAME, 1024, storeActionRejectionThreshold);
            //page 0
            b1.addMessage(generateMessage(SESSION_ALIAS1, Direction.FIRST, 2, 1L));
            b1.addMessage(generateMessage(SESSION_ALIAS1, Direction.SECOND, 3, 2L));
            b1.addMessage(generateMessage(SESSION_ALIAS2, Direction.SECOND, 6, 3L));

            GroupedMessageBatchToStore b2 = new GroupedMessageBatchToStore(GROUP2_NAME, 1024, storeActionRejectionThreshold);
            b2.addMessage(generateMessage(SESSION_ALIAS2, Direction.FIRST, 5, 4L));
            //page 2
            b2.addMessage(generateMessage(SESSION_ALIAS3, Direction.SECOND, 18, 5L));
            b2.addMessage(generateMessage(SESSION_ALIAS4, Direction.SECOND, 19, 6L));

            //page 3
            GroupedMessageBatchToStore b3 = new GroupedMessageBatchToStore(GROUP3_NAME, 1024, storeActionRejectionThreshold);
            b3.addMessage(generateMessage(SESSION_ALIAS4, Direction.FIRST, 25, 7L));
            b3.addMessage(generateMessage(SESSION_ALIAS5, Direction.SECOND, 26, 8L));
            b3.addMessage(generateMessage(SESSION_ALIAS5, Direction.SECOND, 27, 9L));
            b3.addMessage(generateMessage(SESSION_ALIAS6, Direction.SECOND, 28, 10L));

            //page 4
            GroupedMessageBatchToStore b4 = new GroupedMessageBatchToStore(GROUP4_NAME, 1024, storeActionRejectionThreshold);
            b4.addMessage(generateMessage(SESSION_ALIAS6, Direction.FIRST, 35, 11L));
            b4.addMessage(generateMessage(SESSION_ALIAS6, Direction.SECOND, 46, 12L));

            List<GroupedMessageBatchToStore> data = List.of(b1, b2, b3, b4);

            for (GroupedMessageBatchToStore el : data) {
                storage.storeGroupedMessageBatch(el);
            }

        } catch (CradleStorageException | IOException e) {
            logger.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    //------------------------------------------Session Alias test cases------------------------------------------------

    @Test(description = "Simply gets all session aliases for a given book from database")
    public void getAllSessionAliasesTest() throws CradleStorageException, IOException {
        try {
            var actual = storage.getSessionAliases(bookId);
            Assertions.assertThat(actual.size()).isEqualTo(6);
            Assertions.assertThat(actual).hasSameElementsAs(allSessions);
        } catch (IOException | CradleStorageException e) {
            logger.error(e.getMessage(), e);
            throw e;
        }
    }

    @Test(description = "Get session aliases fora a given book and a time interval that covers only one page")
    public void getSessionAliasesOnePageNoDuplicatesTest() throws CradleStorageException {
        try {
            var resultSet = storage.getSessionAliases(bookId, new Interval(dataStart, dataStart.plus(9, ChronoUnit.MINUTES)));
            var actual = newArrayList(resultSet.asIterable());
            Assertions.assertThat(actual.size()).isEqualTo(2);
            var expected = Set.of(SESSION_ALIAS1, SESSION_ALIAS2);
            Assertions.assertThat(actual).hasSameElementsAs(expected);
        } catch (CradleStorageException e) {
            logger.error(e.getMessage(), e);
            throw e;
        }
    }

    @Test(description = "Get session aliases fora a given book and a time interval that covers several pages")
    public void getSessionAliasesMultiPageNoDuplicatesTest() throws CradleStorageException {
        try {
            var resultSet = storage.getSessionAliases(bookId, new Interval(dataStart, dataStart.plus(13, ChronoUnit.MINUTES)));
            var actual = newArrayList(resultSet.asIterable());
            Assertions.assertThat(actual.size()).isEqualTo(4);
            var expected = Set.of(SESSION_ALIAS1, SESSION_ALIAS2, SESSION_ALIAS3, SESSION_ALIAS4);
            Assertions.assertThat(actual).hasSameElementsAs(expected);
        } catch (CradleStorageException e) {
            logger.error(e.getMessage(), e);
            throw e;
        }
    }

    @Test(description = "Get session aliases fora a given book and a time interval that covers several pages and matches partially")
    public void getSessionAliasesMultiPagePartialNoDuplicatesTest() throws CradleStorageException {
        try {
            var resultSet = storage.getSessionAliases(
                    bookId,
                    new Interval(dataStart.plus(7, ChronoUnit.MINUTES), dataStart.plus(33, ChronoUnit.MINUTES))
            );
            var actual = newArrayList(resultSet.asIterable());
            Assertions.assertThat(actual.size()).isEqualTo(6);
            Assertions.assertThat(actual).hasSameElementsAs(allSessions);
        } catch (CradleStorageException e) {
            logger.error(e.getMessage(), e);
            throw e;
        }
    }

    //------------------------------------------Session Group test cases------------------------------------------------
    @Test(description = "Simply gets all group aliases for a given book from database")
    public void getAllGroupAliasesTest() throws CradleStorageException, IOException {
        try {
            var actual = storage.getGroups(bookId);
            Assertions.assertThat(actual.size()).isEqualTo(4);
            Assertions.assertThat(actual).hasSameElementsAs(allGroups);
        } catch (IOException | CradleStorageException e) {
            logger.error(e.getMessage(), e);
            throw e;
        }
    }

    @Test(description = "Get group aliases fora a given book and a time interval that covers only one page")
    public void getGroupAliasesOnePageNoDuplicatesTest() throws CradleStorageException {
        try {
            var resultSet = storage.getSessionGroups(bookId, new Interval(dataStart, dataStart.plus(10, ChronoUnit.MINUTES)));
            var actual = newArrayList(resultSet.asIterable());
            Assertions.assertThat(actual.size()).isEqualTo(2);
            var expected = Set.of(GROUP1_NAME, GROUP2_NAME);
            Assertions.assertThat(actual).hasSameElementsAs(expected);
        } catch (CradleStorageException e) {
            logger.error(e.getMessage(), e);
            throw e;
        }
    }

    @Test(description = "Get group aliases fora a given book and a time interval that covers several pages")
    public void getGroupAliasesMultiPageNoDuplicatesTest() throws CradleStorageException {
        try {
            var resultSet = storage.getSessionGroups(bookId, new Interval(dataStart, dataStart.plus(23, ChronoUnit.MINUTES)));
            var actual = newArrayList(resultSet.asIterable());
            Assertions.assertThat(actual.size()).isEqualTo(3);
            var expected = Set.of(GROUP1_NAME, GROUP2_NAME, GROUP3_NAME);
            Assertions.assertThat(actual).hasSameElementsAs(expected);
        } catch (CradleStorageException e) {
            logger.error(e.getMessage(), e);
            throw e;
        }
    }

    @Test(description = "Get group aliases fora a given book and a time interval that covers several pages and matches partially")
    public void getGroupAliasesMultiPagePartialNoDuplicatesTest() throws CradleStorageException {
        try {
            var resultSet = storage.getSessionGroups(
                    bookId,
                    new Interval(dataStart.plus(7, ChronoUnit.MINUTES), dataStart.plus(33, ChronoUnit.MINUTES))
            );
            var actual = newArrayList(resultSet.asIterable());
            Assertions.assertThat(actual.size()).isEqualTo(4);
            Assertions.assertThat(actual).hasSameElementsAs(allGroups);
        } catch (CradleStorageException e) {
            logger.error(e.getMessage(), e);
            throw e;
        }
    }
}

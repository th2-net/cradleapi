package com.exactpro.cradle.messages;

import com.exactpro.cradle.BookId;
import com.exactpro.cradle.utils.CradleStorageException;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

public class GroupedMessageFilterBuilderTest {

    private static final BookId BOOK_ID = new BookId("test");

    @Test(
            expectedExceptions = CradleStorageException.class,
            expectedExceptionsMessageRegExp = "groupName is blank"
    )
    public void testCheckGroupName() throws CradleStorageException {
        GroupedMessageFilter.builder()
//                .groupName() // skip group name
                .bookId(BOOK_ID)
                .timestampFrom().isGreaterThanOrEqualTo(Instant.MIN)
                .timestampTo().isLessThan(Instant.MAX)
                .build();
    }

    @Test
    public void testCreatesWithOnlyFromTimestamp() throws CradleStorageException {
        Instant from = Instant.now();
        var filter = GroupedMessageFilter.builder()
                .bookId(BOOK_ID)
                .groupName("test")
                .timestampFrom().isGreaterThanOrEqualTo(from)
                .build();

        assertEquals(filter.getFrom().getValue(), from, "unexpected 'from' value");
        assertNull(filter.getTo(), "unexpected 'to' value");
    }

    @Test
    public void testCreatesWithOnlyToTimestamp() throws CradleStorageException {
        Instant to = Instant.now();
        var filter = GroupedMessageFilter.builder()
                .bookId(BOOK_ID)
                .groupName("test")
                .timestampTo().isLessThan(to)
                .build();

        assertEquals(filter.getTo().getValue(), to, "unexpected 'to' value");
        assertNull(filter.getFrom(), "unexpected 'from' value");
    }

    @DataProvider(name = "timestamps")
    public static Object[][] timestamps() {
        return new Object[][]{
                { Instant.now(), Instant.now().plus(1, ChronoUnit.DAYS) },
                { Instant.ofEpochSecond(1000), Instant.ofEpochSecond(1000) }
        };
    }

    @Test(
            dataProvider = "timestamps"
    )
    public void testCreatesWithBothTimestamp(Instant from, Instant to) throws CradleStorageException {
        var filter = GroupedMessageFilter.builder()
                .bookId(BOOK_ID)
                .groupName("test")
                .timestampFrom().isGreaterThanOrEqualTo(from)
                .timestampTo().isLessThan(to)
                .build();

        assertEquals(filter.getFrom().getValue(), from, "unexpected 'from' value");
        assertEquals(filter.getTo().getValue(), to, "unexpected 'to' value");
    }

    @Test(
            expectedExceptions = CradleStorageException.class,
            expectedExceptionsMessageRegExp = "'from' \\(.*\\) must be less or equal to 'to' \\(.*\\)"
    )
    public void testCheckTimestamps() throws CradleStorageException {
        var from = Instant.now();
        var to = from.minus(1, ChronoUnit.DAYS);
        GroupedMessageFilter.builder()
                .bookId(BOOK_ID)
                .groupName("test")
                .timestampFrom().isGreaterThanOrEqualTo(from)
                .timestampTo().isLessThan(to)
                .build();
    }
}
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

package com.exactpro.cradle.messages;

import com.exactpro.cradle.BookId;
import com.exactpro.cradle.Direction;
import com.exactpro.cradle.utils.CradleIdException;
import org.apache.commons.lang3.StringUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.time.Instant;

import static com.exactpro.cradle.messages.StoredMessageId.ID_PARTS_DELIMITER;

public class StoredMessageIdTest {
    private BookId book;
    private String sessionAlias;
    private Direction direction;
    private Instant timestamp;
    private long seq,
            messageSeq;
    private String stringId;

    @BeforeClass
    public void prepare() {
        book = new BookId("book1");
        sessionAlias = "Session1";
        direction = Direction.FIRST;
        timestamp = Instant.EPOCH;
        seq = 100;
        messageSeq = seq + 3;
        stringId = StringUtils.joinWith(ID_PARTS_DELIMITER,
                book,
                sessionAlias,
                direction.getLabel(),
                StoredMessageIdUtils.timestampToString(timestamp),
                messageSeq);
    }

    @DataProvider(name = "escaped")
    public Object[][] escaped() {
        return new Object[][]
                {
                        {new StoredMessageId(new BookId("book1:main"), "127.0.0.1:8080", direction, timestamp, seq)},
                        {new StoredMessageId(new BookId("book1\\final"), "client\\server", direction, timestamp, seq)},
                        {new StoredMessageId(new BookId("book1:main\\client"), "client\\server:9000:21000", direction, timestamp, seq)}
                };
    }

    @DataProvider(name = "invalid IDs")
    public Object[][] invalidIds() {
        return new Object[][]
                {
                        {""},
                        {StringUtils.joinWith(ID_PARTS_DELIMITER, book, sessionAlias, direction.getLabel(), "20211020100000123456789")},
                        {StringUtils.joinWith(ID_PARTS_DELIMITER, book + "\\", sessionAlias, direction.getLabel(), "20211020100000123456789", seq)},
                        {StringUtils.joinWith(ID_PARTS_DELIMITER, "\\" + book, sessionAlias, direction.getLabel(), "20211020100000123456789", seq)},
                        {StringUtils.joinWith(ID_PARTS_DELIMITER, book, sessionAlias, "wrng", "20211020100000123456789", seq)},
                        {StringUtils.joinWith(ID_PARTS_DELIMITER, book, sessionAlias, direction.getLabel(), "tmstmp", seq)},
                        {StringUtils.joinWith(ID_PARTS_DELIMITER, book, sessionAlias, direction.getLabel(), "20211020100000123456789", "seq123")},
                        {StringUtils.joinWith(ID_PARTS_DELIMITER, book, sessionAlias, direction.getLabel(), "20211020100000123456789", seq + "\\")},
                };
    }

    @DataProvider(name = "illegalSequences")
    public Object[][] illegalIndexes() {
        return new Object[][]{
                {Long.MIN_VALUE},
                {-1L},
        };
    }

    @Test
    public void idToString() {
        StoredMessageId id = new StoredMessageId(book, sessionAlias, direction, timestamp, messageSeq);
        Assert.assertEquals(id.toString(), stringId);
    }

    @Test
    public void idFromString() throws CradleIdException {
        StoredMessageId id = new StoredMessageId(book, sessionAlias, direction, timestamp, messageSeq),
                fromString = StoredMessageId.fromString(stringId);
        Assert.assertEquals(fromString, id);
    }

    @Test(dataProvider = "escaped")
    public void idFromEscapedString(StoredMessageId id) throws CradleIdException {
        String s = id.toString();
        StoredMessageId fromString = StoredMessageId.fromString(s);
        Assert.assertEquals(fromString, id);
    }

    @Test(dataProvider = "invalid IDs",
            expectedExceptions = {CradleIdException.class})
    public void idFromStringChecks(String s) throws CradleIdException {
        StoredMessageId.fromString(s);
    }

    @Test
    public void correctSequence() throws CradleIdException {
        StoredMessageId id = StoredMessageId.fromString(stringId);
        Assert.assertEquals(id.getSequence(), messageSeq);
    }

    @Test(
            dataProvider = "illegalSequences",
            expectedExceptions = IllegalArgumentException.class,
            expectedExceptionsMessageRegExp = "illegal sequence -?\\d+ for book1:Session1:1"
    )
    public void testReportIllegalIndex(long sequence) throws CradleIdException {
        stringId = StringUtils.joinWith(ID_PARTS_DELIMITER,
                book,
                sessionAlias,
                direction.getLabel(),
                StoredMessageIdUtils.timestampToString(timestamp),
                sequence);
        StoredMessageId.fromString(stringId);
    }
}

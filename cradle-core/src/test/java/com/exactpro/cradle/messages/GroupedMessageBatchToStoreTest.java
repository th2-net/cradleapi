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
import com.exactpro.cradle.CoreStorageSettings;
import com.exactpro.cradle.Direction;
import com.exactpro.cradle.serialization.MessagesSizeCalculator;
import com.exactpro.cradle.utils.CradleStorageException;
import com.exactpro.cradle.utils.MessageUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;

import static org.testng.Assert.assertEquals;

public class GroupedMessageBatchToStoreTest {
    private final int MAX_SIZE = 1024 * 1024;

    private MessageToStoreBuilder builder;
    private BookId book;
    private String sessionAlias;
    private String groupName;
    private Direction direction;
    private Instant timestamp;
    private String protocol;
    private byte[] messageContent;

    private final long storeActionRejectionThreshold = new CoreStorageSettings().calculateStoreActionRejectionThreshold();

    @BeforeClass
    public void prepare() {
        builder = new MessageToStoreBuilder();
        book = new BookId("test-book");
        sessionAlias = "test-session";
        groupName = "test-group";
        direction = Direction.FIRST;
        this.protocol = "test-protocol";
        timestamp = Instant.now().minus(1, ChronoUnit.DAYS);
        messageContent = "Message text".getBytes();
    }

    @DataProvider(name = "multiple messages")
    public Object[][] multipleMessages() {
        Direction d = Direction.FIRST;
        long seq = 10;
        return new Object[][]
                {
                        {Arrays.asList(new IdData(book, sessionAlias, d, timestamp, seq),
                                new IdData(new BookId(book.getName() + "X"), sessionAlias, d, timestamp, seq + 1))},             //Different books
                        {Arrays.asList(new IdData(book, sessionAlias, d, timestamp, seq),
                                new IdData(book, sessionAlias, d, timestamp.minusMillis(1), seq))},    //Timestamp is less than previous
                        {Arrays.asList(new IdData(book, sessionAlias, d, timestamp, seq),
                                new IdData(book, sessionAlias, d, timestamp, seq),                     //Sequence is not incremented
                                new IdData(book, sessionAlias, d, timestamp, seq - 1))},        //Sequence is less than previous
                };
    }

    @DataProvider(name = "invalid messages")
    public Object[][] invalidMessages() {
        return new Object[][]
                {
                        {builder},                                                                                       //Empty message
                        {builder.bookId(null)},                                                                             //Null book
                        {builder.bookId(new BookId(""))},                                                         //Empty book
                        {builder.bookId(book)},                                                                          //Only book is set
                        {builder.bookId(book).sessionAlias(null).direction(null).timestamp(null)},                     //Null session is set
                        {builder.bookId(book).sessionAlias("").direction(null).timestamp(null)},                         //Empty session is set
                        {builder.bookId(book).sessionAlias(sessionAlias).direction(null).timestamp(null)},               //Only book and session are set
                        {builder.bookId(book).sessionAlias(sessionAlias).direction(direction).timestamp(null)},          //Only book, session and direction are set
                        {builder.bookId(book).sessionAlias(sessionAlias).direction(direction).timestamp(Instant.now())}  //Content is not set
                };
    }


    @Test(expectedExceptions = {CradleStorageException.class}, expectedExceptionsMessageRegExp = "Batch has not enough space to hold given message")
    public void batchContentIsLimited() throws CradleStorageException {
        byte[] content = new byte[5000];
        GroupedMessageBatchToStore batch = new GroupedMessageBatchToStore(groupName, MAX_SIZE, storeActionRejectionThreshold);
        batch.addMessage(builder
                .bookId(book)
                .sessionAlias(sessionAlias)
                .direction(direction)
                .sequence(1)
                .timestamp(timestamp)
                .content(content)
                .build());
        for (int i = 0; i <= (MAX_SIZE / content.length) + 1; i++)
            batch.addMessage(builder
                    .bookId(book)
                    .sessionAlias(sessionAlias)
                    .direction(direction)
                    .timestamp(timestamp)
                    .content(content)
                    .build());
    }

    @Test
    public void batchIsFull1() throws CradleStorageException {
        GroupedMessageBatchToStore fullBySizeBatch = GroupedMessageBatchToStoreJoinTest.createFullBySizeBatch(book, sessionAlias,
                1, direction, timestamp, groupName, protocol);

        Assert.assertTrue(fullBySizeBatch.isFull(), "Batch indicates it is full");
        assertEquals(fullBySizeBatch.getBatchSize(), GroupedMessageBatchToStoreJoinTest.MAX_SIZE);
        assertEquals(fullBySizeBatch.getSpaceLeft(), 0);
    }

    @Test
    public void batchIsFull() throws CradleStorageException {
        byte[] content = new byte[MAX_SIZE / 2];
        GroupedMessageBatchToStore batch = new GroupedMessageBatchToStore(groupName, MAX_SIZE, storeActionRejectionThreshold);
        batch.addMessage(builder
                .bookId(book)
                .sessionAlias(sessionAlias)
                .direction(direction)
                .sequence(1)
                .timestamp(timestamp)
                .content(content)
                .protocol(protocol)
                .build());
        batch.addMessage(builder
                .bookId(book)
                .sessionAlias(sessionAlias)
                .direction(direction)
                .timestamp(timestamp)
                .protocol(protocol)
                .content(new byte[MAX_SIZE - MessagesSizeCalculator.MESSAGE_BATCH_CONST_VALUE
                        - MessagesSizeCalculator.MESSAGE_LENGTH_IN_BATCH * 2
                        - MessagesSizeCalculator.MESSAGE_SIZE_CONST_VALUE * 2
                        - MessagesSizeCalculator.calculateStringSize(sessionAlias) * 2
                        - MessagesSizeCalculator.calculateStringSize(direction.getLabel()) * 2
                        - MessagesSizeCalculator.calculateStringSize(protocol) * 2
                        - content.length])
                .build());

        Assert.assertTrue(batch.isFull(), "Batch indicates it is full");
    }

    @Test
    public void batchCountsSpaceLeft() throws CradleStorageException {
        byte[] content = new byte[MAX_SIZE / 2];
        GroupedMessageBatchToStore batch = new GroupedMessageBatchToStore(groupName, MAX_SIZE, storeActionRejectionThreshold);
        long left = batch.getSpaceLeft();

        MessageToStore msg = builder
                .bookId(book)
                .sessionAlias(sessionAlias)
                .direction(direction)
                .sequence(1)
                .timestamp(timestamp)
                .content(content)
                .build();

        batch.addMessage(msg);

        assertEquals(batch.getSpaceLeft(), left - MessagesSizeCalculator.calculateMessageSizeInBatch(msg), "Batch counts space left");
    }

    @Test
    public void batchChecksSpaceLeft() throws CradleStorageException {
        byte[] content = new byte[MAX_SIZE / 2];
        GroupedMessageBatchToStore batch = new GroupedMessageBatchToStore(groupName, MAX_SIZE, storeActionRejectionThreshold);

        MessageToStore msg = builder
                .bookId(book)
                .sessionAlias(sessionAlias)
                .direction(direction)
                .sequence(1)
                .timestamp(timestamp)
                .content(content)
                .build();
        batch.addMessage(msg);

        Assert.assertFalse(batch.hasSpace(msg), "Batch shows if it has space to hold given message");
    }


    @Test(expectedExceptions = {IllegalArgumentException.class},
            expectedExceptionsMessageRegExp = "illegal sequence -?\\d+ for test-book:test-session:1")
    public void batchChecksFirstMessage() throws CradleStorageException {
        GroupedMessageBatchToStore batch = new GroupedMessageBatchToStore(groupName, MAX_SIZE, storeActionRejectionThreshold);
        batch.addMessage(builder
                .bookId(book)
                .sessionAlias(sessionAlias)
                .direction(direction)
                .sequence(-1)
                .timestamp(timestamp)
                .content(messageContent)
                .build());
    }

    @Test(dataProvider = "multiple messages",
            expectedExceptions = {CradleStorageException.class},
            expectedExceptionsMessageRegExp = ".*, but in your message it is .*")
    public void batchConsistency(List<IdData> ids) throws CradleStorageException {
        GroupedMessageBatchToStore batch = new GroupedMessageBatchToStore(groupName, MAX_SIZE, storeActionRejectionThreshold);
        for (IdData id : ids) {
            batch.addMessage(builder
                    .bookId(id.book)
                    .sessionAlias(id.sessionAlias)
                    .direction(id.direction)
                    .sequence(id.sequence)
                    .timestamp(id.timestamp)
                    .content(messageContent)
                    .build());
        }
    }

    @Test(dataProvider = "invalid messages",
            expectedExceptions = {CradleStorageException.class},
            expectedExceptionsMessageRegExp = "Message must .*")
    public void messageValidation(MessageToStoreBuilder builder) throws CradleStorageException {
        MessageBatchToStore batch = new MessageBatchToStore(MAX_SIZE, storeActionRejectionThreshold);
        batch.addMessage(builder.build());
    }

    @Test
    public void sequenceAutoIncrement() throws CradleStorageException {
        long seq = 10;
        MessageBatchToStore batch = MessageBatchToStore.singleton(builder
                .bookId(book)
                .sessionAlias(sessionAlias)
                .direction(direction)
                .sequence(seq)
                .timestamp(timestamp)
                .content(messageContent)
                .build(), MAX_SIZE, storeActionRejectionThreshold);

        StoredMessage msg1 = batch.addMessage(builder
                .bookId(book)
                .sessionAlias(sessionAlias)
                .direction(direction)
                .timestamp(timestamp)
                .content(messageContent)
                .build());

        StoredMessage msg2 = batch.addMessage(builder
                .bookId(book)
                .sessionAlias(sessionAlias)
                .direction(direction)
                .timestamp(timestamp)
                .content(messageContent)
                .build());

        assertEquals(msg1.getSequence(), seq + 1, "1st and 2nd messages should have ordered sequence numbers");
        assertEquals(msg2.getSequence(), msg1.getSequence() + 1, "2nd and 3rd messages should have ordered sequence numbers");
    }

    @Test
    public void sequenceGapsAllowed() throws CradleStorageException {
        long seq = 10;
        MessageBatchToStore batch = MessageBatchToStore.singleton(builder
                .bookId(book)
                .sessionAlias(sessionAlias)
                .direction(direction)
                .sequence(seq)
                .timestamp(timestamp)
                .content(messageContent)
                .build(), MAX_SIZE, storeActionRejectionThreshold);

        batch.addMessage(builder
                .bookId(book)
                .sessionAlias(sessionAlias)
                .direction(direction)
                .sequence(seq + 10)
                .timestamp(timestamp)
                .content(messageContent)
                .build());
    }

    @Test
    public void correctMessageId() throws CradleStorageException {
        long seq = 10;
        MessageBatchToStore batch = MessageBatchToStore.singleton(builder
                .bookId(book)
                .sessionAlias(sessionAlias)
                .direction(direction)
                .sequence(seq)
                .timestamp(timestamp)
                .content(messageContent)
                .build(), MAX_SIZE, storeActionRejectionThreshold);
        StoredMessage msg = batch.addMessage(builder
                .bookId(book)
                .sessionAlias(sessionAlias)
                .direction(direction)
                .timestamp(timestamp)
                .content(messageContent)
                .build());
        assertEquals(msg.getId(), new StoredMessageId(book, sessionAlias, direction, timestamp, seq + 1));
    }

    @Test
    public void batchShowsFirstLastTimestamp() throws CradleStorageException {
        Instant timestamp = Instant.ofEpochSecond(1000);
        GroupedMessageBatchToStore batch = new GroupedMessageBatchToStore(groupName, MAX_SIZE, storeActionRejectionThreshold);
        batch.addMessage(builder
                .bookId(book)
                .sessionAlias(sessionAlias)
                .direction(Direction.FIRST)
                .sequence(1)
                .timestamp(timestamp.plusSeconds(20))
                .content(messageContent)
                .build());

        batch.addMessage(builder
                .bookId(book)
                .sessionAlias(sessionAlias)
                .direction(Direction.SECOND)
                .timestamp(timestamp.plusSeconds(10))
                .content(messageContent)
                .build());

        batch.addMessage(builder
                .bookId(book)
                .sessionAlias(sessionAlias)
                .direction(Direction.SECOND)
                .timestamp(timestamp.plusSeconds(15))
                .content(messageContent)
                .build());

        assertEquals(batch.getFirstTimestamp(), timestamp.plusSeconds(10), "First timestamp is incorrect");
        assertEquals(batch.getLastTimestamp(), timestamp.plusSeconds(20), "Last timestamp is incorrect");
    }

    @Test
    public void batchSerialization() throws CradleStorageException, IOException {
        GroupedMessageBatchToStore batch = new GroupedMessageBatchToStore(groupName, MAX_SIZE, storeActionRejectionThreshold);
        batch.addMessage(builder
                .bookId(book)
                .sessionAlias(sessionAlias)
                .direction(direction)
                .sequence(0)
                .timestamp(timestamp)
                .metadata("md", "some value")
                .content(messageContent)
                .build());

        StoredMessage storedMsg = batch.getFirstMessage();
        byte[] bytes = MessageUtils.serializeMessages(batch).getSerializedData();
        StoredMessage msg = MessageUtils.deserializeMessages(bytes, book).iterator().next();
        assertEquals(msg, storedMsg, "Message should be completely serialized/deserialized");
    }


    static class IdData {
        final BookId book;
        final String sessionAlias;
        final Direction direction;
        final Instant timestamp;
        final long sequence;

        public IdData(BookId book, String sessionAlias, Direction direction, Instant timestamp, long sequence) {
            this.book = book;
            this.sessionAlias = sessionAlias;
            this.direction = direction;
            this.timestamp = timestamp;
            this.sequence = sequence;
        }

        @Override
        public String toString() {
            return book + StoredMessageId.ID_PARTS_DELIMITER
                    + sessionAlias + StoredMessageId.ID_PARTS_DELIMITER
                    + direction.getLabel() + StoredMessageId.ID_PARTS_DELIMITER
                    + StoredMessageIdUtils.timestampToString(timestamp) + StoredMessageId.ID_PARTS_DELIMITER
                    + sequence;
        }
    }
}

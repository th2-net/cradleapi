/*
 * Copyright 2020-2022 Exactpro (Exactpro Systems Limited)
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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.time.Instant;

import com.exactpro.cradle.serialization.MessagesSizeCalculator;
import com.exactpro.cradle.serialization.SerializationException;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.exactpro.cradle.Direction;
import com.exactpro.cradle.BookId;
import com.exactpro.cradle.utils.CradleStorageException;

public class MessageBatchToStoreJoinTest
{
    private static final BookId bookId = new BookId("testbook");
    static final int MAX_SIZE = 1024;

    @Test
    public void testJoinEmptyBatchWithOther() throws CradleStorageException, SerializationException {
        MessageBatchToStore emptyBatch = createEmptyBatch();
        MessageBatchToStore batch = createBatch(bookId, "test", 1, Direction.FIRST, Instant.EPOCH, 5, 5);
        assertTrue(emptyBatch.addBatch(batch));

        assertEquals(emptyBatch.getMessageCount(), 5);
        assertEquals(emptyBatch.getBatchSize(), getBatchSize(emptyBatch));
        assertEquals(emptyBatch.getSessionAlias(), "test");
        assertEquals(emptyBatch.getDirection(), Direction.FIRST);
    }
    
    @Test(dataProvider = "full batches")
    public void testJoinEmptyBatchWithFull(MessageBatchToStore other) throws CradleStorageException {
        MessageBatchToStore emptyBatch = createEmptyBatch();
        assertTrue(emptyBatch.addBatch(other));

        assertTrue(emptyBatch.isFull());
        assertEquals(emptyBatch.getSessionAlias(), "test");
        assertEquals(emptyBatch.getDirection(), Direction.FIRST);
    }

    
    @DataProvider(name = "full batches")
    public Object[][] fullBatches() throws CradleStorageException {
        return new Object[][] {
                { createFullBySizeBatch(bookId, "test", 0, Direction.FIRST, Instant.EPOCH) }
        };
    }

    @Test(dataProvider = "full batches")
    public void testJoinFullBatchWithEmpty(MessageBatchToStore batch) throws CradleStorageException {
        long messagesSize = batch.getBatchSize();
        int messageCount = batch.getMessageCount();
        StoredMessageId id = batch.getId();

        assertTrue(batch.addBatch(createEmptyBatch()));

        assertEquals(batch.getMessageCount(), messageCount);
        assertEquals(batch.getBatchSize(), messagesSize);
        assertEquals(batch.getId(), id);
    }

    @DataProvider(name = "full batches matrix")
    public Object[][] fullBatchesMatrix() throws CradleStorageException {
        MessageBatchToStore fullBySizeBatch = createFullBySizeBatch(bookId, "test", 0, Direction.FIRST, Instant.EPOCH);
        return new Object[][] {
                { fullBySizeBatch, fullBySizeBatch }
        };
    }

    @Test(dataProvider = "full batches matrix")
    public void testFullBatches(MessageBatchToStore first, MessageBatchToStore second) throws CradleStorageException {
        assertFalse(first.addBatch(second));
    }


    @Test
    public void testAddBatchLessThanLimit() throws CradleStorageException, SerializationException {
        MessageBatchToStore first = createBatch(bookId, "test", 0, Direction.FIRST, Instant.EPOCH, 5, 5);
        MessageBatchToStore second = createBatch(bookId, "test", 5, Direction.FIRST, Instant.EPOCH.plusMillis(5), 5, 5);

        assertEquals(first.getBatchSize(), getBatchSize(first));
        assertTrue(first.addBatch(second));
        assertEquals(first.getMessageCount(), 10);
        assertEquals(first.getBatchSize(), getBatchSize(first));
        assertEquals(first.getSessionAlias(), "test");
        assertEquals(first.getDirection(), Direction.FIRST);
    }

    @Test
    public void testAddBatchMoreThanLimitBySize() throws CradleStorageException, SerializationException {
        MessageBatchToStore first = createBatch(bookId, "test", 0, Direction.FIRST, Instant.EPOCH, 1, MAX_SIZE / 2);
        MessageBatchToStore second = createBatch(bookId, "test", 5, Direction.FIRST, Instant.EPOCH, 1, MAX_SIZE / 2);

        long sizeBefore = first.getBatchSize();
        assertFalse(first.addBatch(second));
        assertEquals(first.getMessageCount(), 1);
        assertEquals(first.getBatchSize(), sizeBefore);
        assertEquals(first.getBatchSize(), getBatchSize(first));
        assertEquals(first.getSessionAlias(), "test");
        assertEquals(first.getDirection(), Direction.FIRST);
    }
    
    
    @Test(
        expectedExceptions = CradleStorageException.class,
        expectedExceptionsMessageRegExp = "IDs are not compatible.*"
    )
    public void testThrowExceptionOnDifferentBooks() throws CradleStorageException {
        MessageBatchToStore first = createBatch(bookId, "testA", 0, Direction.FIRST, Instant.EPOCH, 5, 5);
        MessageBatchToStore
				second = createBatch(new BookId(bookId.getName()+"2"), "testA", 5, Direction.FIRST, Instant.EPOCH, 5, 5);

        first.addBatch(second);
    }

    @Test(
            expectedExceptions = CradleStorageException.class,
            expectedExceptionsMessageRegExp = "IDs are not compatible.*"
    )
    public void testThrowExceptionOnDifferentSessions() throws CradleStorageException {
        MessageBatchToStore first = createBatch(bookId, "testA", 0, Direction.FIRST, Instant.EPOCH, 5, 5);
        MessageBatchToStore second = createBatch(bookId, "testB", 5, Direction.FIRST, Instant.EPOCH, 5, 5);

        first.addBatch(second);
    }

    @Test(
            expectedExceptions = CradleStorageException.class,
            expectedExceptionsMessageRegExp = "IDs are not compatible.*"
    )
    public void testThrowExceptionOnDifferentDirections() throws CradleStorageException {
        MessageBatchToStore first = createBatch(bookId, "test", 0, Direction.FIRST, Instant.EPOCH, 5, 5);
        MessageBatchToStore second = createBatch(bookId, "test", 5, Direction.SECOND, Instant.EPOCH, 5, 5);

        first.addBatch(second);
    }
    
    @Test(
            expectedExceptions = CradleStorageException.class,
            expectedExceptionsMessageRegExp = "Batches are not ordered.*"
    )
    public void testThrowExceptionOnUnorderedTimestamps() throws CradleStorageException {
        MessageBatchToStore first = createBatch(bookId, "test", 0, Direction.FIRST, Instant.EPOCH.plusMillis(5), 5, 5);
        MessageBatchToStore second = createBatch(bookId, "test", 5, Direction.FIRST, Instant.EPOCH, 5, 5);

        first.addBatch(second);
    }

    @Test(
            expectedExceptions = CradleStorageException.class,
            expectedExceptionsMessageRegExp = "Batches are not ordered.*"
    )
    public void testThrowExceptionOnUnorderedSequences() throws CradleStorageException {
        MessageBatchToStore first = createBatch(bookId, "test", 5, Direction.FIRST, Instant.EPOCH, 5, 5);
        MessageBatchToStore second = createBatch(bookId, "test", 0, Direction.FIRST, Instant.EPOCH, 5, 5);

        first.addBatch(second);
    }

    private static MessageBatchToStore createBatch(BookId bookId, String sessionAlias, long startSequence, Direction direction, Instant startTimestamp,
            int messageCount, int contentSizePerMessage) throws CradleStorageException {
        MessageBatchToStore messageBatchToStore = createEmptyBatch();
        long begin = startSequence;
        Instant timestamp = startTimestamp;
        while (messageCount-- > 0) {
            MessageToStoreBuilder toStore = MessageToStore.builder()
                    .bookId(bookId)
                    .sessionAlias(sessionAlias)
                    .direction(direction)
                    .timestamp(timestamp)
                    .sequence(begin++);
            if (contentSizePerMessage > 0) {
                toStore = toStore.content(new byte[contentSizePerMessage]);
            }
            messageBatchToStore.addMessage(toStore.build());
            
            timestamp = timestamp.plusMillis(1);
        }
        return messageBatchToStore;
    }

    private static MessageBatchToStore createEmptyBatch() {
        return new MessageBatchToStore(MAX_SIZE);
    }

    static MessageBatchToStore createFullBySizeBatch(BookId bookId,
            String sessionAlias, long startSequence, Direction direction, Instant startTimestamp) throws CradleStorageException {
        return createBatch(bookId, sessionAlias, startSequence, direction, startTimestamp, 1,
                MAX_SIZE - (MessagesSizeCalculator.MESSAGE_BATCH_CONST_VALUE
                        + MessagesSizeCalculator.MESSAGE_SIZE_CONST_VALUE + MessagesSizeCalculator.MESSAGE_LENGTH_IN_BATCH
                + MessagesSizeCalculator.calculateStringSize(sessionAlias) + MessagesSizeCalculator.calculateStringSize(direction.getLabel())));
    }

    private int getBatchSize(StoredMessageBatch batch) throws SerializationException {
        return MessagesSizeCalculator.calculateMessageBatchSize(batch.getMessages()).total;
    }
}

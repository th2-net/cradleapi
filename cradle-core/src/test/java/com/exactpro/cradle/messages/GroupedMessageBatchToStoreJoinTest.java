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

import com.exactpro.cradle.BookId;
import com.exactpro.cradle.Direction;
import com.exactpro.cradle.serialization.MessagesSizeCalculator;
import com.exactpro.cradle.serialization.SerializationException;
import com.exactpro.cradle.utils.CradleStorageException;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.time.Instant;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class GroupedMessageBatchToStoreJoinTest
{
    private static final BookId bookId = new BookId("test-book");
    private static final String groupName = "test-group";
    private static final String protocol = "test-protocol";
    static final int MAX_SIZE = 1024;

    @Test
    public void testJoinEmptyBatchWithOther() throws CradleStorageException, SerializationException {
        GroupedMessageBatchToStore emptyBatch = createEmptyBatch(groupName);
        GroupedMessageBatchToStore batch = createBatch(bookId, "test", 1, Direction.FIRST, Instant.EPOCH, 5, 5, groupName, null);
        assertTrue(emptyBatch.addBatch(batch));

        assertEquals(emptyBatch.getMessageCount(), 5);
        assertEquals(emptyBatch.getBatchSize(), emptyBatch.getBatchSize());
        assertEquals(emptyBatch.getGroup(), groupName);
    }
    
    @Test(dataProvider = "full batches")
    public void testJoinEmptyBatchWithFull(GroupedMessageBatchToStore other) throws CradleStorageException {
        GroupedMessageBatchToStore emptyBatch = createEmptyBatch(groupName);
        assertTrue(emptyBatch.addBatch(other));

        assertTrue(emptyBatch.isFull());
        assertEquals(emptyBatch.getGroup(), groupName);
    }

    
    @DataProvider(name = "full batches")
    public Object[][] fullBatches() throws CradleStorageException {
        return new Object[][] {
                { createFullBySizeBatch(bookId, "test", 0, Direction.FIRST, Instant.EPOCH, groupName, protocol) }
        };
    }

    @Test(dataProvider = "full batches")
    public void testJoinFullBatchWithEmpty(GroupedMessageBatchToStore batch) throws CradleStorageException {
        long messagesSize = batch.getBatchSize();
        int messageCount = batch.getMessageCount();

        assertTrue(batch.addBatch(createEmptyBatch(groupName)));

        assertEquals(batch.getGroup(), groupName);
        assertEquals(batch.getMessageCount(), messageCount);
        assertEquals(batch.getBatchSize(), messagesSize);
    }

    @DataProvider(name = "full batches matrix")
    public Object[][] fullBatchesMatrix() throws CradleStorageException {
        GroupedMessageBatchToStore fullBySizeBatch = createFullBySizeBatch(bookId, "test", 0, Direction.FIRST, Instant.EPOCH, groupName, protocol);
        return new Object[][] {
                { fullBySizeBatch, fullBySizeBatch }
        };
    }

    @Test(dataProvider = "full batches matrix")
    public void testFullBatches(GroupedMessageBatchToStore first, GroupedMessageBatchToStore second) throws CradleStorageException {
        assertFalse(first.addBatch(second));
    }


    @Test
    public void testAddBatchLessThanLimit() throws CradleStorageException, SerializationException {
        GroupedMessageBatchToStore first = createBatch(bookId, "test", 0, Direction.FIRST, Instant.EPOCH, 5, 5, groupName, protocol);
        GroupedMessageBatchToStore second = createBatch(bookId, "test", 5, Direction.FIRST, Instant.EPOCH.plusMillis(5), 5, 5, groupName, protocol);

        assertEquals(first.getBatchSize(), first.getBatchSize());
        assertTrue(first.addBatch(second));
        assertEquals(first.getMessageCount(), 10);
        assertEquals(first.getBatchSize(), first.getBatchSize());
        assertEquals(first.getGroup(), groupName);
    }

    @Test
    public void testAddBatchMoreThanLimitBySize() throws CradleStorageException, SerializationException {
        GroupedMessageBatchToStore first = createBatch(bookId, "test", 0, Direction.FIRST, Instant.EPOCH, 1, MAX_SIZE / 2, groupName, protocol);
        GroupedMessageBatchToStore second = createBatch(bookId, "test", 5, Direction.FIRST, Instant.EPOCH, 1, MAX_SIZE / 2, groupName, protocol);

        long sizeBefore = first.getBatchSize();
        assertFalse(first.addBatch(second));
        assertEquals(first.getMessageCount(), 1);
        assertEquals(first.getBatchSize(), sizeBefore);
        assertEquals(first.getBatchSize(), first.getBatchSize());
        assertEquals(first.getGroup(), groupName);
    }
    
    
    @Test(
        expectedExceptions = CradleStorageException.class,
        expectedExceptionsMessageRegExp = "Batch BookId-s differ.*"
    )
    public void testThrowExceptionOnDifferentBooks() throws CradleStorageException {
        GroupedMessageBatchToStore first = createBatch(bookId, "testA", 0, Direction.FIRST, Instant.EPOCH, 5, 5, groupName, protocol);
        GroupedMessageBatchToStore second =
                createBatch(new BookId(bookId.getName()+"2"), "testA", 5, Direction.FIRST, Instant.EPOCH, 5, 5, groupName, protocol);

        first.addBatch(second);
    }

    @Test(
            expectedExceptions = CradleStorageException.class,
            expectedExceptionsMessageRegExp = "Batch groups differ.*"
    )
    public void testThrowExceptionOnDifferentGroups() throws CradleStorageException {
        GroupedMessageBatchToStore first = createBatch(bookId, "test", 0, Direction.FIRST, Instant.EPOCH, 5, 5, "test-group-1", protocol);
        GroupedMessageBatchToStore second = createBatch(bookId, "test", 5, Direction.FIRST, Instant.EPOCH, 5, 5, "test-group-2", protocol);

        first.addBatch(second);
    }

    @Test
    public void testJoinDifferentSessions() throws CradleStorageException {
        GroupedMessageBatchToStore first = createBatch(bookId, "testA", 0, Direction.FIRST, Instant.EPOCH, 5, 5, groupName, protocol);
        GroupedMessageBatchToStore second = createBatch(bookId, "testB", 5, Direction.SECOND, Instant.EPOCH.plusSeconds(100), 5, 5, groupName, protocol);

        first.addBatch(second);
    }
    
    @Test(
            expectedExceptions = CradleStorageException.class,
            expectedExceptionsMessageRegExp = "Batches intersect by time.*"
    )
    public void testThrowExceptionOnUnorderedTimestamps() throws CradleStorageException {
        GroupedMessageBatchToStore first = createBatch(bookId, "test", 0, Direction.FIRST, Instant.EPOCH.plusMillis(5), 5, 5, groupName, protocol);
        GroupedMessageBatchToStore second = createBatch(bookId, "test", 5, Direction.FIRST, Instant.EPOCH, 5, 5, groupName, protocol);

        first.addBatch(second);
    }

    @Test(
            expectedExceptions = CradleStorageException.class,
            expectedExceptionsMessageRegExp = "Batches are not ordered.*"
    )
    public void testThrowExceptionOnUnorderedSequences() throws CradleStorageException {
        GroupedMessageBatchToStore first = createBatch(bookId, "test", 5, Direction.FIRST, Instant.EPOCH, 5, 5, groupName, protocol);
        GroupedMessageBatchToStore second = createBatch(bookId, "test", 0, Direction.FIRST, Instant.EPOCH.plusSeconds(10), 5, 5, groupName, protocol);

        first.addBatch(second);
    }

    private static GroupedMessageBatchToStore createBatch(BookId bookId, String sessionAlias, long startSequence, Direction direction, Instant startTimestamp,
            int messageCount, int contentSizePerMessage, String group, String protocol) throws CradleStorageException {
        GroupedMessageBatchToStore batch = createEmptyBatch(group);
        long begin = startSequence;
        Instant timestamp = startTimestamp;
        while (messageCount-- > 0) {
            MessageToStoreBuilder toStore = MessageToStore.builder()
                    .bookId(bookId)
                    .sessionAlias(sessionAlias)
                    .direction(direction)
                    .timestamp(timestamp)
                    .protocol(protocol)
                    .sequence(begin++);
            if (contentSizePerMessage > 0) {
                toStore = toStore.content(new byte[contentSizePerMessage]);
            }
            batch.addMessage(toStore.build());
            
            timestamp = timestamp.plusMillis(1);
        }
        return batch;
    }

    private static GroupedMessageBatchToStore createEmptyBatch(String group) {
        return new GroupedMessageBatchToStore(group, MAX_SIZE);
    }

    static GroupedMessageBatchToStore createFullBySizeBatch(BookId bookId,
            String sessionAlias, long startSequence, Direction direction, Instant startTimestamp, String group, String protocol) throws CradleStorageException {
        return createBatch(bookId,
                sessionAlias,
                startSequence,
                direction,
                startTimestamp,
                1,
                MAX_SIZE - ( MessagesSizeCalculator.MESSAGE_BATCH_CONST_VALUE +
                                MessagesSizeCalculator.MESSAGE_SIZE_CONST_VALUE +
                                MessagesSizeCalculator.MESSAGE_LENGTH_IN_BATCH +
                                MessagesSizeCalculator.calculateStringSize(sessionAlias) +
                                MessagesSizeCalculator.calculateStringSize(protocol) +
                                MessagesSizeCalculator.calculateStringSize(direction.getLabel())),
                group,
                protocol);
    }

}

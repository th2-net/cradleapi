/*
 * Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
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

import static com.exactpro.cradle.messages.StoredMessageBatch.DEFAULT_MAX_BATCH_SIZE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.time.Instant;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.exactpro.cradle.Direction;
import com.exactpro.cradle.utils.CradleStorageException;

public class StoredMessageBatchJoinTest {

    @Test
    public void testJoinEmptyBatchWithOther() throws CradleStorageException {
        StoredMessageBatch emptyBatch = createEmptyBatch();
        StoredMessageBatch batch = createBatch("test", 1, Direction.FIRST, 5, 5);
        assertTrue(emptyBatch.addBatch(batch));

        assertEquals(emptyBatch.getMessageCount(), 5);
        assertEquals(emptyBatch.getBatchSize(), 25);
        assertEquals(emptyBatch.getStreamName(), "test");
        assertEquals(emptyBatch.getDirection(), Direction.FIRST);
    }

    @Test(dataProvider = "full batches")
    public void testJoinEmptyBatchWithFull(StoredMessageBatch other) throws CradleStorageException {
        StoredMessageBatch emptyBatch = createEmptyBatch();
        assertTrue(emptyBatch.addBatch(other));

        assertTrue(emptyBatch.isFull());
        assertEquals(emptyBatch.getStreamName(), "test");
        assertEquals(emptyBatch.getDirection(), Direction.FIRST);
    }

    @DataProvider(name = "full batches")
    public Object[][] fullBatches() throws CradleStorageException {
        return new Object[][] {
                { createFullBySizeBatch("test", 0, Direction.FIRST) }
        };
    }

    @Test(dataProvider = "full batches")
    public void testJoinFullBatchWithEmpty(StoredMessageBatch batch) throws CradleStorageException {
        long messagesSize = batch.getBatchSize();
        int messageCount = batch.getMessageCount();
        StoredMessageBatchId id = batch.getId();

        assertTrue(batch.addBatch(createEmptyBatch()));

        assertEquals(batch.getMessageCount(), messageCount);
        assertEquals(batch.getBatchSize(), messagesSize);
        assertEquals(batch.getId(), id);
    }

    @DataProvider(name = "full batches matrix")
    public Object[][] fullBatchesMatrix() throws CradleStorageException {
        StoredMessageBatch fullBySizeBatch = createFullBySizeBatch("test", 0, Direction.FIRST);
        return new Object[][] {
                { fullBySizeBatch, fullBySizeBatch }
        };
    }

    @Test(dataProvider = "full batches matrix")
    public void testFullBatches(StoredMessageBatch first, StoredMessageBatch second) throws CradleStorageException {
        assertFalse(first.addBatch(second));
    }

    @Test
    public void testAddBatchLessThanLimit() throws CradleStorageException {
        StoredMessageBatch first = createBatch("test", 0, Direction.FIRST, 5, 5);
        StoredMessageBatch second = createBatch("test", 5, Direction.FIRST, 5, 5);

        assertTrue(first.addBatch(second));
        assertEquals(first.getMessageCount(), 10);
        assertEquals(first.getBatchSize(), 50);
        assertEquals(first.getStreamName(), "test");
        assertEquals(first.getDirection(), Direction.FIRST);
    }

    @Test
    public void testAddBatchMoreThanLimitBySize() throws CradleStorageException {
        StoredMessageBatch first = createBatch("test", 0, Direction.FIRST, 1, DEFAULT_MAX_BATCH_SIZE - 1);
        StoredMessageBatch second = createBatch("test", 5, Direction.FIRST, 1, DEFAULT_MAX_BATCH_SIZE - 1);

        assertFalse(first.addBatch(second));
        assertEquals(first.getMessageCount(), 1);
        assertEquals(first.getBatchSize(), DEFAULT_MAX_BATCH_SIZE - 1);
        assertEquals(first.getStreamName(), "test");
        assertEquals(first.getDirection(), Direction.FIRST);
    }

    @Test(
            expectedExceptions = CradleStorageException.class,
            expectedExceptionsMessageRegExp = "IDs are not compatible.*"
    )
    public void testThrowExceptionOnDifferentStreams() throws CradleStorageException {
        StoredMessageBatch first = createBatch("testA", 0, Direction.FIRST, 5, 5);
        StoredMessageBatch second = createBatch("testB", 5, Direction.FIRST, 5, 5);

        first.addBatch(second);
    }

    @Test(
            expectedExceptions = CradleStorageException.class,
            expectedExceptionsMessageRegExp = "IDs are not compatible.*"
    )
    public void testThrowExceptionOnDifferentDirections() throws CradleStorageException {
        StoredMessageBatch first = createBatch("test", 0, Direction.FIRST, 5, 5);
        StoredMessageBatch second = createBatch("test", 5, Direction.SECOND, 5, 5);

        first.addBatch(second);
    }

    @Test(
            expectedExceptions = CradleStorageException.class,
            expectedExceptionsMessageRegExp = "Batches are not ordered.*"
    )
    public void testThrowExceptionOnUnorderedSequences() throws CradleStorageException {
        StoredMessageBatch first = createBatch("test", 5, Direction.FIRST, 5, 5);
        StoredMessageBatch second = createBatch("test", 0, Direction.FIRST, 5, 5);

        first.addBatch(second);
    }

    private StoredMessageBatch createBatch(String steamName, long startIndex, Direction direction, int messageCount, int contentSizePerMessage) throws CradleStorageException {
        StoredMessageBatch storedMessageBatch = createEmptyBatch();
        long begin = startIndex;
        while (messageCount-- > 0) {
            MessageToStore toStore = new MessageToStore();
            toStore.setIndex(begin++);
            toStore.setDirection(direction);
            toStore.setStreamName(steamName);
            toStore.setTimestamp(Instant.EPOCH);
            if (contentSizePerMessage > 0) {
                toStore.setContent(new byte[contentSizePerMessage]);
            }
            storedMessageBatch.addMessage(toStore);
        }
        return storedMessageBatch;
    }

    private StoredMessageBatch createEmptyBatch() {
        return new StoredMessageBatch();
    }

    private StoredMessageBatch createFullBySizeBatch(String streamName, long startIndex, Direction direction) throws CradleStorageException {
        return createBatch(streamName, startIndex, direction, 1, DEFAULT_MAX_BATCH_SIZE);
    }
}

/*
 * Copyright 2020-2021 Exactpro (Exactpro Systems Limited)
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
import static org.testng.Assert.assertTrue;

import java.time.Instant;

import org.testng.annotations.Test;

import com.exactpro.cradle.Direction;
import com.exactpro.cradle.BookId;
import com.exactpro.cradle.utils.CradleStorageException;

public class StoredMessageBatchJoinTest {
    private static final BookId bookId = new BookId("testbook");
    
    @Test
    public void testJoinEmptyBatchWithOther() throws CradleStorageException {
        StoredMessageBatch emptyBatch = createEmptyBatch();
        StoredMessageBatch batch = createBatch(bookId, "test", 1, Direction.FIRST, Instant.EPOCH, 5, 5);
        assertTrue(emptyBatch.addBatch(batch));

        assertEquals(emptyBatch.getMessageCount(), 5);
        assertEquals(emptyBatch.getBatchSize(), 25);
        assertEquals(emptyBatch.getSessionAlias(), "test");
        assertEquals(emptyBatch.getDirection(), Direction.FIRST);
    }

    @Test
    public void testAddBatch() throws CradleStorageException {
        StoredMessageBatch first = createBatch(bookId, "test", 0, Direction.FIRST, Instant.EPOCH, 5, 5);
        StoredMessageBatch second = createBatch(bookId, "test", 5, Direction.FIRST, Instant.EPOCH.plusMillis(5), 5, 5);

        assertTrue(first.addBatch(second));
        assertEquals(first.getMessageCount(), 10);
        assertEquals(first.getBatchSize(), 50);
        assertEquals(first.getSessionAlias(), "test");
        assertEquals(first.getDirection(), Direction.FIRST);
    }
    
    
    @Test(
        expectedExceptions = CradleStorageException.class,
        expectedExceptionsMessageRegExp = "IDs are not compatible.*"
    )
    public void testThrowExceptionOnDifferentBooks() throws CradleStorageException {
        StoredMessageBatch first = createBatch(bookId, "testA", 0, Direction.FIRST, Instant.EPOCH, 5, 5);
        StoredMessageBatch second = createBatch(new BookId(bookId.getName()+"2"), "testA", 5, Direction.FIRST, Instant.EPOCH, 5, 5);

        first.addBatch(second);
    }

    @Test(
            expectedExceptions = CradleStorageException.class,
            expectedExceptionsMessageRegExp = "IDs are not compatible.*"
    )
    public void testThrowExceptionOnDifferentSessions() throws CradleStorageException {
        StoredMessageBatch first = createBatch(bookId, "testA", 0, Direction.FIRST, Instant.EPOCH, 5, 5);
        StoredMessageBatch second = createBatch(bookId, "testB", 5, Direction.FIRST, Instant.EPOCH, 5, 5);

        first.addBatch(second);
    }

    @Test(
            expectedExceptions = CradleStorageException.class,
            expectedExceptionsMessageRegExp = "IDs are not compatible.*"
    )
    public void testThrowExceptionOnDifferentDirections() throws CradleStorageException {
        StoredMessageBatch first = createBatch(bookId, "test", 0, Direction.FIRST, Instant.EPOCH, 5, 5);
        StoredMessageBatch second = createBatch(bookId, "test", 5, Direction.SECOND, Instant.EPOCH, 5, 5);

        first.addBatch(second);
    }
    
    @Test(
            expectedExceptions = CradleStorageException.class,
            expectedExceptionsMessageRegExp = "Batches are not ordered.*"
    )
    public void testThrowExceptionOnUnorderedTimestamps() throws CradleStorageException {
        StoredMessageBatch first = createBatch(bookId, "test", 0, Direction.FIRST, Instant.EPOCH.plusMillis(5), 5, 5);
        StoredMessageBatch second = createBatch(bookId, "test", 5, Direction.FIRST, Instant.EPOCH, 5, 5);

        first.addBatch(second);
    }

    @Test(
            expectedExceptions = CradleStorageException.class,
            expectedExceptionsMessageRegExp = "Batches are not ordered.*"
    )
    public void testThrowExceptionOnUnorderedSequences() throws CradleStorageException {
        StoredMessageBatch first = createBatch(bookId, "test", 5, Direction.FIRST, Instant.EPOCH, 5, 5);
        StoredMessageBatch second = createBatch(bookId, "test", 0, Direction.FIRST, Instant.EPOCH, 5, 5);

        first.addBatch(second);
    }

    private StoredMessageBatch createBatch(BookId bookId, String sessionAlias, long startSequence, Direction direction, Instant startTimestamp, 
            int messageCount, int contentSizePerMessage) throws CradleStorageException {
        StoredMessageBatch storedMessageBatch = createEmptyBatch();
        long begin = startSequence;
        Instant timestamp = startTimestamp;
        while (messageCount-- > 0) {
            MessageToStore toStore = new MessageToStore();
            toStore.setBook(bookId);
            toStore.setSequence(begin++);
            toStore.setDirection(direction);
            toStore.setSessionAlias(sessionAlias);
            toStore.setTimestamp(timestamp);
            if (contentSizePerMessage > 0) {
                toStore.setContent(new byte[contentSizePerMessage]);
            }
            storedMessageBatch.addMessage(toStore);
            
            timestamp = timestamp.plusMillis(1);
        }
        return storedMessageBatch;
    }

    private StoredMessageBatch createEmptyBatch() {
        return new StoredMessageBatch();
    }
}

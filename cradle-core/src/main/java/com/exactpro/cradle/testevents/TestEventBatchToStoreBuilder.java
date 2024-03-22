/*
 * Copyright 2020-2024 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.cradle.testevents;

import com.exactpro.cradle.BookId;
import com.exactpro.cradle.serialization.EventsSizeCalculator;
import com.exactpro.cradle.utils.CradleStorageException;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.exactpro.cradle.serialization.EventsSizeCalculator.getRecordSizeInBatch;

/**
 * Builder for {@link TestEventBatchToStore} object. After calling {@link #build()} method, the builder can be reused to build new test event
 */
public class TestEventBatchToStoreBuilder extends TestEventToStoreBuilder {
    private final int maxBatchSize;
    private final Set<StoredTestEventId> eventIds = new HashSet<>();
    private final List<TestEventSingleToStore> eventsWithAttachedMessages = new ArrayList<>();
    private final List<TestEventSingleToStore> events = new ArrayList<>();
    private int batchSize = EventsSizeCalculator.EVENT_BATCH_LEN_CONST;
    private boolean success = true;
    private Instant endTimestamp = null;

    public TestEventBatchToStoreBuilder(int maxBatchSize, long storeActionRejectionThreshold) {
        super(storeActionRejectionThreshold);
        this.maxBatchSize = maxBatchSize;
    }


    public TestEventBatchToStoreBuilder id(StoredTestEventId id) {
        checkEvents(id, events);
        super.id(id);
        return this;
    }

    public TestEventBatchToStoreBuilder id(BookId book, String scope, Instant startTimestamp, String id) {
        super.id(book, scope, startTimestamp, id);
        return this;
    }

    public TestEventBatchToStoreBuilder idRandom(BookId book, String scope) {
        super.idRandom(book, scope);
        return this;
    }

    public TestEventBatchToStoreBuilder parentId(StoredTestEventId parentId) {
        checkParentEventIds(parentId, eventIds, events);
        super.parentId(parentId);
        return this;
    }

    /**
     * Adds test event to the batch. Batch will verify the event to match batch conditions.
     * Result of this method should be used for all further operations on the event
     *
     * @param event to add to the batch
     * @return immutable test event object
     * @throws CradleStorageException if test event cannot be added to the batch due to verification failure
     */
    public TestEventBatchToStoreBuilder addTestEvent(TestEventSingleToStore event) throws CradleStorageException {
        int currEventSize = getRecordSizeInBatch(event);
        if (!hasSpace(currEventSize))
            throw new CradleStorageException("Batch has not enough space to hold given test event");

        checkEvent(this.id, event);

        StoredTestEventId parentId = event.getParentId();
        if (parentId == null) {
            throw new CradleStorageException("Event being added to batch must have a parent. "
                    + "It can be parent of the batch itself or another event already stored in this batch");
        }
        checkParentEventId(this.parentId, eventIds, parentId);

        if (!event.isSuccess()) {
            success = false;
        }
        Instant endTimestamp = event.getEndTimestamp();
        if (endTimestamp != null && (this.endTimestamp == null || this.endTimestamp.isBefore(endTimestamp))) {
            this.endTimestamp = endTimestamp;
        }

        if (!eventIds.add(event.getId())) {
            throw new CradleStorageException("Test event with ID '" + event.getId() + "' is already present in batch");
        }
        events.add(event);
        batchSize += currEventSize;
        if (event.hasMessages()) {
            eventsWithAttachedMessages.add(event);
        }
        return this;
    }

    /**
     * Shows if batch has enough space to hold given test event
     *
     * @param event to check against batch capacity
     * @return true if batch has enough space to hold given test event
     */
    public boolean hasSpace(TestEventSingleToStore event) {
        return hasSpace(getRecordSizeInBatch(event));
    }

    /**
     * @return size of events currently stored in the batch
     */
    public int getBatchSize() {
        return batchSize;
    }

    /**
     * Indicates if the batch cannot hold more test events
     *
     * @return true if batch capacity is reached and the batch must be flushed to Cradle
     */
    public boolean isFull() {
        return batchSize >= maxBatchSize;
    }

    /**
     * Shows how many bytes the batch can hold till its capacity is reached
     *
     * @return number of bytes the batch can hold
     */
    public int getSpaceLeft() {
        int result = maxBatchSize - batchSize;
        return Math.max(result, 0);
    }

    public TestEventBatchToStore build() throws CradleStorageException {
        try {

            return new TestEventBatchToStore(
                    id,
                    parentId,
                    endTimestamp,
                    success,
                    events,
                    eventsWithAttachedMessages,
                    batchSize
            );
        } finally {
            reset();
        }
    }

    protected void reset() {
        super.reset();
        batchSize = EventsSizeCalculator.EVENT_BATCH_LEN_CONST;
        success = true;
        endTimestamp = null;
        eventsWithAttachedMessages.clear();
        eventIds.clear();
        events.clear();
    }

    private boolean hasSpace(int eventLen) {
        return batchSize + eventLen <= maxBatchSize;
    }

    private static void checkEvents(StoredTestEventId id, Collection<TestEventSingleToStore> events) {
        if (id == null || events == null || events.isEmpty()) {
            return;
        }
        BookId book = id.getBookId();
        String scope = id.getScope();
        Instant startTimestamp = id.getStartTimestamp();

        for (TestEventSingle event : events) {
            checkEvent(event, book, scope, startTimestamp);
        }
    }

    private static void checkParentEventId(StoredTestEventId batchParentId,
                                           Set<StoredTestEventId> knownIds,
                                           StoredTestEventId newParentId) throws CradleStorageException {
        if (batchParentId == null || newParentId == null) {
            return;
        }
        if(!(batchParentId.equals(newParentId) || knownIds.contains(newParentId))) {
            throw new CradleStorageException("Test event with ID '" + newParentId + "' should be parent of the batch itself or "
                    + "should be stored in this batch to be referenced as a parent");
        }
    }
    private static void checkParentEventIds(StoredTestEventId batchParentId,
                                            Set<StoredTestEventId> knownIds,
                                            Collection<TestEventSingleToStore> events) {
        if (batchParentId == null || events == null || events.isEmpty()) {
            return;
        }
        for (TestEventSingleToStore event : events) {
            if(!(batchParentId.equals(event.getParentId()) || knownIds.contains(event.getParentId()))) {
                throw new IllegalArgumentException("Test event with ID '" + event.getParentId() + "' should be parent of the batch itself or "
                        + "should be stored in this batch to be referenced as a parent");
            }
        }
    }

    private static void checkEvent(TestEventSingle event, BookId book, String scope, Instant startTimestamp) {
        if (!book.equals(event.getBookId())) {
            throw new IllegalArgumentException("Batch contains events of book '" + book + "', "
                    + "but in your event it is '" + event.getBookId() + "'");
        }
        if (!scope.equals(event.getScope())) {
            throw new IllegalArgumentException("Batch contains events of scope '" + scope + "', "
                    + "but in your event it is '" + event.getScope() + "'");
        }
        if (event.getStartTimestamp().isBefore(startTimestamp)) {
            throw new IllegalArgumentException("Start timestamp of event (" + event.getStartTimestamp() +
                    ") is before the batch start timestamp (" + startTimestamp + ')');
        }
    }

    private static void checkEvent(StoredTestEventId id, TestEventSingle event) throws CradleStorageException {
        if (id == null || event == null) {
            return;
        }
        checkEvent(event, id.getBookId(), id.getScope(), id.getStartTimestamp());
    }
}

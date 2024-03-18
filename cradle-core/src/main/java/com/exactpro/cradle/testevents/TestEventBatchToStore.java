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

import com.exactpro.cradle.messages.StoredMessageId;
import com.exactpro.cradle.utils.CradleStorageException;

import javax.annotation.Nonnull;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Holds information about batch of test events prepared to be stored in Cradle
 * Events stored in the batch can refer to each other to form a hierarchy. No references to these events are possible outside the batch and vice versa.
 * Root events in the batch should reference batch's parent.
 */
public class TestEventBatchToStore extends TestEventToStore {
    private final Collection<TestEventSingleToStore> eventsWithAttachedMessages;
    private final Collection<TestEventSingleToStore> events;
    private final int batchSize;
    TestEventBatchToStore(@Nonnull StoredTestEventId id,
                          @Nonnull StoredTestEventId parentId,
                          Instant endTimestamp,
                          boolean success,
                          Collection<TestEventSingleToStore> events,
                          Collection<TestEventSingleToStore> eventsWithAttachedMessages,
                          int batchSize) throws CradleStorageException {
        super(id,
                "",
                requireNonNull(parentId, "Parent event id can't be null"),
                "",
                endTimestamp,
                success
        );
        if (events == null || events.isEmpty()) {
            throw new CradleStorageException("Batch " + id + " is empty");
        }
        if (batchSize < 1) {
            throw new CradleStorageException("Batch " + id + " size can't be negative " + batchSize);
        }
        this.events = List.copyOf(events);
        this.batchSize = batchSize;
        this.eventsWithAttachedMessages = List.copyOf(eventsWithAttachedMessages);
    }

    public static TestEventBatchToStoreBuilder builder(int maxBatchSize, long storeActionRejectionThreshold) {
        return new TestEventBatchToStoreBuilder(maxBatchSize, storeActionRejectionThreshold);
    }


    @SuppressWarnings("ConstantConditions")
    @Override
    public Set<StoredMessageId> getMessages() {
        throw new UnsupportedOperationException();
    }

    public Collection<TestEventSingleToStore> getEventsWithAttachedMessages() {
        return eventsWithAttachedMessages;
    }

    public int getTestEventsCount() {
        return events.size();
    }

    public Collection<TestEventSingleToStore> getTestEvents() {
        return events;
    }

    /**
     * @return size of events currently stored in the batch
     */
    public int getBatchSize() {
        return batchSize;
    }
}

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
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Holds information about batch of test events prepared to be stored in Cradle
 * Events stored in the batch can refer to each other to form a hierarchy. No references to these events are possible outside the batch and vice versa.
 * Root events in the batch should reference batch's parent.
 */
public class TestEventBatchToStore extends TestEventToStore implements TestEventBatch {
    private final Map<StoredTestEventId, BatchedStoredTestEvent> events;
    private final Collection<BatchedStoredTestEvent> rootEvents;
    private final Map<StoredTestEventId, Collection<BatchedStoredTestEvent>> children;
    private final Map<StoredTestEventId, Set<StoredMessageId>> messages;
    private final Set<StoredMessageId> batchMessages;
    private final int batchSize;

    TestEventBatchToStore(@Nonnull StoredTestEventId id,
                          @Nonnull StoredTestEventId parentId,
                          @Nonnull Instant endTimestamp,
                          boolean success,
                          @Nonnull Map<StoredTestEventId, TestEventSingleToStore> events,
                          int batchSize) throws CradleStorageException {
        super(id,
                "",
                requireNonNull(parentId, "Parent event id can't be null"),
                "",
                requireNonNull(endTimestamp, "End timestamp can't be null"),
                success
        );

        Map<StoredTestEventId, BatchedStoredTestEvent> idToEvent = new LinkedHashMap<>();
        Collection<BatchedStoredTestEvent> rootEvents = new ArrayList<>();
        Map<StoredTestEventId, Collection<BatchedStoredTestEvent>> children = new HashMap<>();
        Map<StoredTestEventId, Set<StoredMessageId>> messages = new HashMap<>();
        Set<StoredMessageId> batchMessages = new HashSet<>();

        for (TestEventSingleToStore event : events.values()) {
            BatchedStoredTestEvent batchedEvent = new BatchedStoredTestEvent(event, this, null, event.getSize());
            if(idToEvent.putIfAbsent(event.getId(), batchedEvent) != null) {
                throw new CradleStorageException("Test event with ID '" + event.getId() + "' is already present in batch");
            }
            messages.put(event.getId(), event.getMessages());
            batchMessages.addAll(event.getMessages());

            if(parentId.equals(event.getParentId())) {
                rootEvents.add(batchedEvent);
            } else {
                children.computeIfAbsent(parentId, k -> new ArrayList<>()).add(batchedEvent);
            }
        }

        this.events = Collections.unmodifiableMap(idToEvent);
        this.rootEvents = Collections.unmodifiableCollection(rootEvents);
        this.children = Collections.unmodifiableMap(children);
        this.messages = Collections.unmodifiableMap(messages);
        this.batchMessages = Collections.unmodifiableSet(batchMessages);
        this.batchSize = batchSize;
    }


    public static TestEventBatchToStoreBuilder builder(int maxBatchSize, long storeActionRejectionThreshold) {
        return new TestEventBatchToStoreBuilder(maxBatchSize, storeActionRejectionThreshold);
    }


    @SuppressWarnings("ConstantConditions")
    @Override
    public Set<StoredMessageId> getMessages() {
        return batchMessages;
    }

    @Override
    public int getTestEventsCount() {
        return events.size();
    }

    @Override
    public BatchedStoredTestEvent getTestEvent(StoredTestEventId id) {
        return events.get(id);
    }

    @Override
    public Collection<BatchedStoredTestEvent> getTestEvents() {
        return events.values();
    }

    @Override
    public Collection<BatchedStoredTestEvent> getRootTestEvents() {
        return rootEvents;
    }

    @Override
    public Map<StoredTestEventId, Set<StoredMessageId>> getBatchMessages() {
        return messages;
    }

    @Override
    public boolean hasChildren(StoredTestEventId parentId) {
        return children.containsKey(parentId);
    }

    @Override
    public Collection<BatchedStoredTestEvent> getChildren(StoredTestEventId parentId) {
        Collection<BatchedStoredTestEvent> result = children.get(parentId);
        return result != null ? Collections.unmodifiableCollection(result) : Collections.emptyList();
    }

    @Override
    public Set<StoredMessageId> getMessages(StoredTestEventId eventId) {
        Set<StoredMessageId> result = messages.get(eventId);
        return result != null ? result : Collections.emptySet();
    }

    /**
     * @return size of events currently stored in the batch
     */
    public int getBatchSize() {
        return batchSize;
    }
}

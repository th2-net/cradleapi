/*
 * Copyright 2025 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.cradle.testevents.lw;

import com.exactpro.cradle.PageId;
import com.exactpro.cradle.messages.StoredMessageId;
import com.exactpro.cradle.testevents.StoredTestEvent;
import com.exactpro.cradle.testevents.StoredTestEventId;
import com.exactpro.cradle.utils.CradleStorageException;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Holds information about batch of test events stored in Cradle.
 * Events stored in the batch can refer to each other to form a hierarchy. No references to these events are possible outside of the batch and vice versa.
 * Root events in the batch should reference batch's parent.
 */
public class LwStoredTestEventBatch extends StoredTestEvent implements LwTestEventBatch {
    private final Map<StoredTestEventId, LwBatchedStoredTestEvent> events;
    private final Collection<LwBatchedStoredTestEvent> rootEvents;
    private final Map<StoredTestEventId, Collection<LwBatchedStoredTestEvent>> children;
    private final Map<StoredTestEventId, Set<StoredMessageId>> messages;
    private final Instant endTimestamp;
    private final boolean success;
    private Instant lastStartTimestamp;

    public LwStoredTestEventBatch(StoredTestEventId id, String name, String type, StoredTestEventId parentId,
                                  Collection<LwBatchedStoredTestEvent> batchEvents,
                                  Map<StoredTestEventId, Set<StoredMessageId>> messages,
                                  PageId pageId, String error, Instant recDate) throws CradleStorageException {
        super(id, name, type, parentId, pageId, error, recDate);

        Map<StoredTestEventId, LwBatchedStoredTestEvent> allEvents = new LinkedHashMap<>();
        List<LwBatchedStoredTestEvent> roots = new ArrayList<>();
        Map<StoredTestEventId, Collection<LwBatchedStoredTestEvent>> childrenPerEvent = new LinkedHashMap<>();
        Map<StoredTestEventId, Set<StoredMessageId>> batchMessages = new HashMap<>();
        Instant end = null;
        boolean success = true;
        if (batchEvents != null) {
            for (LwBatchedStoredTestEvent event : batchEvents) {
                StoredTestEventId eventParentId = event.getParentId();
                if (eventParentId == null)
                    throw new CradleStorageException("Child event must have a parent: batch parent or another event from batch");

                boolean isRoot = Objects.equals(eventParentId, getParentId());

                LwBatchedStoredTestEvent child = new LwBatchedStoredTestEvent(event, this, pageId);
                allEvents.put(child.getId(), child);
                if (!isRoot)
                    childrenPerEvent.computeIfAbsent(eventParentId, k -> new ArrayList<>()).add(child);
                else
                    roots.add(child);

                Set<StoredMessageId> eventMessages = messages != null ? messages.get(child.getId()) : null;
                if (eventMessages != null)
                    batchMessages.put(child.getId(), Set.copyOf(eventMessages));

                Instant eventEnd = child.getEndTimestamp();
                if (eventEnd != null) {
                    if (end == null || end.isBefore(eventEnd))
                        end = eventEnd;
                }

                if (!child.isSuccess())
                    success = false;
            }
        }

        this.events = Collections.unmodifiableMap(allEvents);
        this.rootEvents = Collections.unmodifiableList(roots);
        this.children = Collections.unmodifiableMap(childrenPerEvent);
        this.messages = Collections.unmodifiableMap(batchMessages);
        this.endTimestamp = end;
        this.success = success;
        getLastStartTimestamp();
    }

    public LwStoredTestEventBatch(LwTestEventBatch batch, PageId pageId) throws CradleStorageException {
        this(batch.getId(), batch.getName(), batch.getType(), batch.getParentId(),
                batch.getTestEvents(), batch.getBatchMessages(), pageId, null, null);
    }

    @Override
    public Instant getEndTimestamp() {
        return endTimestamp;
    }

    @Override
    public boolean isSuccess() {
        return success;
    }

    @Override
    public Set<StoredMessageId> getMessages() {
        Set<StoredMessageId> result = new HashSet<>();
        messages.values().forEach(result::addAll);
        return result;
    }


    @Override
    public int getTestEventsCount() {
        return events.size();
    }

    @Override
    public LwBatchedStoredTestEvent getTestEvent(StoredTestEventId id) {
        return events.get(id);
    }

    @Override
    public Collection<LwBatchedStoredTestEvent> getTestEvents() {
        return events.values();
    }

    @Override
    public Collection<LwBatchedStoredTestEvent> getRootTestEvents() {
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
    public Collection<LwBatchedStoredTestEvent> getChildren(StoredTestEventId parentId) {
        Collection<LwBatchedStoredTestEvent> result = children.get(parentId);
        return result != null ? Collections.unmodifiableCollection(result) : Collections.emptyList();
    }

    @Override
    public Set<StoredMessageId> getMessages(StoredTestEventId eventId) {
        Set<StoredMessageId> result = messages.get(eventId);
        return result != null ? result : Collections.emptySet();
    }

    @Override
    public Instant getLastStartTimestamp() {
        if (lastStartTimestamp == null) {
            lastStartTimestamp = getStartTimestamp();

            for (LwBatchedStoredTestEvent el : getTestEvents()) {
                lastStartTimestamp = lastStartTimestamp.isBefore(el.getStartTimestamp()) ? el.getStartTimestamp() : lastStartTimestamp;
            }

            return lastStartTimestamp;
        }

        return lastStartTimestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof LwStoredTestEventBatch)) return false;
        LwStoredTestEventBatch batch = (LwStoredTestEventBatch) o;
        return isSuccess() == batch.isSuccess()
                && Objects.equals(events, batch.events)
                && rootEvents.containsAll(batch.rootEvents)
                && batch.rootEvents.containsAll(rootEvents)
                && Objects.equals(children, batch.children)
                && Objects.equals(getMessages(), batch.getMessages())
                && Objects.equals(getEndTimestamp(), batch.getEndTimestamp())
                && Objects.equals(getLastStartTimestamp(), batch.getLastStartTimestamp());
    }

    @Override
    public int hashCode() {
        return Objects.hash(events, rootEvents, children, getMessages(), getEndTimestamp(), isSuccess(), getLastStartTimestamp());
    }

    @Override
    public String toString() {
        return "StoredTestEventBatch{" +
                "events=" + events +
                ", rootEvents=" + rootEvents +
                ", children=" + children +
                ", messages=" + messages +
                ", endTimestamp=" + endTimestamp +
                ", success=" + success +
                ", lastStartTimestamp=" + lastStartTimestamp +
                '}';
    }
}

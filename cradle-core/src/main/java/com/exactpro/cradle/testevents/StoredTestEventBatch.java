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

import com.exactpro.cradle.PageId;
import com.exactpro.cradle.messages.StoredMessageId;
import com.exactpro.cradle.utils.CradleStorageException;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Holds information about batch of test events stored in Cradle.
 * Events stored in the batch can refer to each other to form a hierarchy. No references to these events are possible outside the batch and vice versa.
 * Root events in the batch should reference batch's parent.
 */
public class StoredTestEventBatch extends StoredTestEvent implements TestEventBatch
{
	private final Map<StoredTestEventId, BatchedStoredTestEvent> events;
	private final Collection<BatchedStoredTestEvent> rootEvents;
	private final Map<StoredTestEventId, Collection<BatchedStoredTestEvent>> children;
	private final Map<StoredTestEventId, Set<StoredMessageId>> messages;
	private final Instant endTimestamp;
	private final boolean success;
	private Instant lastStartTimestamp;

	public StoredTestEventBatch(StoredTestEventId id, String name, String type, StoredTestEventId parentId,
			Collection<BatchedStoredTestEvent> batchEvents, 
			Map<StoredTestEventId, Set<StoredMessageId>> messages, 
			PageId pageId, String error, Instant recDate) throws CradleStorageException {
		super(id, name, type, parentId, pageId, error, recDate);
		
		Map<StoredTestEventId, BatchedStoredTestEvent> allEvents = new LinkedHashMap<>();
		List<BatchedStoredTestEvent> roots = new ArrayList<>();
		Map<StoredTestEventId, Collection<BatchedStoredTestEvent>> childrenPerEvent = new LinkedHashMap<>();
		Instant end = null;
		boolean success = true;
		if (batchEvents != null)
		{
			for (BatchedStoredTestEvent event : batchEvents)
			{
				StoredTestEventId eventParentId = event.getParentId();
				if (eventParentId == null)
					throw new CradleStorageException("Child event must have a parent: batch parent or another event from batch");
				
				boolean isRoot = Objects.equals(eventParentId, getParentId());
				
				BatchedStoredTestEvent child = new BatchedStoredTestEvent(event, this, pageId, event.getSize());
				allEvents.put(child.getId(), child);
				if (!isRoot)
					childrenPerEvent.computeIfAbsent(eventParentId, k -> new ArrayList<>()).add(child);
				else
					roots.add(child);
				
				Instant eventEnd = child.getEndTimestamp();
				if (eventEnd != null)
				{
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
		this.messages = Collections.unmodifiableMap(messages);
		this.endTimestamp = end;
		this.success = success;
		getLastStartTimestamp();
	}
	
	public StoredTestEventBatch(TestEventBatch batch, PageId pageId) throws CradleStorageException
	{
		this(batch.getId(), batch.getName(), batch.getType(), batch.getParentId(),
				batch.getTestEvents(), batch.getBatchMessages(), pageId, null, null);
	}
	
	@Override
	public Instant getEndTimestamp()
	{
		return endTimestamp;
	}
	
	@Override
	public boolean isSuccess()
	{
		return success;
	}
	
	@Override
	public Set<StoredMessageId> getMessages()
	{
		Set<StoredMessageId> result = new HashSet<>();
		messages.values().forEach(result::addAll);
		return result;
	}
	
	
	@Override
	public int getTestEventsCount()
	{
		return events.size();
	}
	
	@Override
	public BatchedStoredTestEvent getTestEvent(StoredTestEventId id)
	{
		return events.get(id);
	}
	
	@Override
	public Collection<BatchedStoredTestEvent> getTestEvents()
	{
		return events.values();
	}
	
	@Override
	public Collection<BatchedStoredTestEvent> getRootTestEvents()
	{
		return rootEvents;
	}
	
	@Override
	public Map<StoredTestEventId, Set<StoredMessageId>> getBatchMessages()
	{
		return messages;
	}
	
	@Override
	public boolean hasChildren(StoredTestEventId parentId)
	{
		return children.containsKey(parentId);
	}
	
	@Override
	public Collection<BatchedStoredTestEvent> getChildren(StoredTestEventId parentId)
	{
		Collection<BatchedStoredTestEvent> result = children.get(parentId);
		return result != null ? Collections.unmodifiableCollection(result) : Collections.emptyList();
	}
	
	@Override
	public Set<StoredMessageId> getMessages(StoredTestEventId eventId)
	{
		Set<StoredMessageId> result = messages.get(eventId);
		return result != null ? result : Collections.emptySet();
	}

	@Override
	public Instant getLastStartTimestamp() {
		if (lastStartTimestamp == null) {
			lastStartTimestamp = getStartTimestamp();

			for (BatchedStoredTestEvent el : getTestEvents()) {
				lastStartTimestamp = lastStartTimestamp.isBefore(el.getStartTimestamp()) ? el.getStartTimestamp() : lastStartTimestamp;
			}

			return lastStartTimestamp;
		}

		return lastStartTimestamp;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (!(o instanceof StoredTestEventBatch)) return false;
		StoredTestEventBatch batch = (StoredTestEventBatch) o;
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

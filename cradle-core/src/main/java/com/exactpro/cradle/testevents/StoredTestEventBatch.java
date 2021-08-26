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

package com.exactpro.cradle.testevents;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import com.exactpro.cradle.PageId;
import com.exactpro.cradle.messages.StoredMessageId;
import com.exactpro.cradle.utils.CradleStorageException;

/**
 * Holds information about batch of test events stored in Cradle.
 * Events stored in the batch can refer to each other to form a hierarchy. No references to these events are possible outside of the batch and vice versa.
 * Root events in the batch should reference batch's parent.
 */
public class StoredTestEventBatch extends StoredTestEvent implements TestEventBatch
{
	private final Map<StoredTestEventId, BatchedStoredTestEvent> events;
	private final Collection<BatchedStoredTestEvent> rootEvents;
	private final Map<StoredTestEventId, Collection<BatchedStoredTestEvent>> children;
	private final Instant endTimestamp;
	private final boolean success;
	private final Set<StoredMessageId> messages;
	
	public StoredTestEventBatch(StoredTestEventId id, String name, String type, StoredTestEventId parentId,
			Collection<BatchedStoredTestEvent> batchEvents, PageId pageId) throws CradleStorageException
	{
		super(id, name, type, parentId, pageId);
		
		Map<StoredTestEventId, BatchedStoredTestEvent> allEvents = new LinkedHashMap<>();
		Collection<BatchedStoredTestEvent> roots = new ArrayList<>();
		Map<StoredTestEventId, Collection<BatchedStoredTestEvent>> childrenPerEvent = new LinkedHashMap<>();
		Set<StoredMessageId> batchMessages = new HashSet<>();
		Instant end = null;
		boolean success = true;
		for (BatchedStoredTestEvent event : batchEvents)
		{
			StoredTestEventId eventParentId = event.getParentId();
			if (eventParentId == null)
				throw new CradleStorageException("Child event must have a parent: batch parent or another event from batch");
			
			boolean isRoot = Objects.equals(eventParentId, getParentId());
			
			BatchedStoredTestEvent child = new BatchedStoredTestEvent(event, this, pageId);
			allEvents.put(child.getId(), child);
			if (!isRoot)
				childrenPerEvent.computeIfAbsent(eventParentId, k -> new ArrayList<>()).add(child);
			else
				roots.add(child);
			
			Set<StoredMessageId> eventMessages = child.getMessages();
			if (eventMessages != null)
				batchMessages.addAll(eventMessages);
			
			Instant eventEnd = child.getEndTimestamp();
			if (eventEnd != null)
			{
				if (end == null || end.isBefore(eventEnd))
					end = eventEnd;
			}
			
			if (!child.isSuccess())
				success = false;
		}
		
		this.events = Collections.unmodifiableMap(allEvents);
		this.rootEvents = Collections.unmodifiableCollection(roots);
		this.children = Collections.unmodifiableMap(childrenPerEvent);
		this.endTimestamp = end;
		this.success = success;
		
		this.messages = batchMessages != null && batchMessages.size() > 0 ? Collections.unmodifiableSet(batchMessages) : null;
	}
	
	public StoredTestEventBatch(TestEventBatch batch, PageId pageId) throws CradleStorageException
	{
		this(batch.getId(), batch.getName(), batch.getType(), batch.getParentId(),
				batch.getTestEvents(), pageId);
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
		return messages;
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
}

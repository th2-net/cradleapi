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

package com.exactpro.cradle.testevents;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

import com.exactpro.cradle.utils.CradleStorageException;
import com.exactpro.cradle.utils.TestEventUtils;

/**
 * Holds information about batch of test events stored in Cradle.
 * Events stored in the batch can refer to each other to form a hierarchy. No references to these events are possible outside of the batch and vice versa.
 * Root events in the batch should reference batch's parent.
 * Batch has limited capacity. If batch is full, events can't be added to it and the batch must be flushed to Cradle
 */
public class StoredTestEventBatch extends MinimalStoredTestEvent implements StoredTestEvent
{
	public static int MAX_EVENTS_NUMBER = 100,
			MAX_EVENTS_SIZE = 1024*1024;  //1 Mb
	
	private final Map<StoredTestEventId, BatchedStoredTestEvent> events = new LinkedHashMap<>();
	private final Collection<BatchedStoredTestEvent> rootEvents = new ArrayList<>();
	private final Map<StoredTestEventId, Collection<BatchedStoredTestEvent>> children = new HashMap<>();
	private Instant startTimestamp,
			endTimestamp;
	private boolean success;
	private long storedEventsSize = 0;
	
	public StoredTestEventBatch(TestEventBatchToStore batchData) throws CradleStorageException
	{
		super(batchData.getId() != null ? batchData.getId() : new StoredTestEventId(UUID.randomUUID().toString()),
				batchData.getName(),
				batchData.getType(),
				batchData.getParentId());
		if (getParentId() == null)
			throw new CradleStorageException("Batch must have a parent");
	}
	
	
	public static BatchedStoredTestEvent bindTestEvent(BatchedStoredTestEvent event, StoredTestEventBatch batch)
	{
		return new BatchedStoredTestEvent(event, batch);
	}
	
	public static BatchedStoredTestEvent addTestEvent(StoredTestEventWithContent event, StoredTestEventBatch batch) throws CradleStorageException
	{
		return batch.addStoredTestEvent(event);
	}

	
	@Override
	public Instant getStartTimestamp()
	{
		return startTimestamp;
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
	
	
	/**
	 * @return number of test events currently stored in the batch
	 */
	public int getTestEventsCount()
	{
		return events.size();
	}
	
	/**
	 * @return size of events content currently stored in the batch
	 */
	public long getTestEventsSize()
	{
		return storedEventsSize;
	}
	
	/**
	 * Returns test event stored in the batch by ID
	 * @param id of test event to get from batch
	 * @return test event for given ID, if it is present in the batch, null otherwise
	 */
	public BatchedStoredTestEvent getTestEvent(StoredTestEventId id)
	{
		return events.get(id);
	}
	
	/**
	 * @return collection of test events stored in the batch
	 */
	public Collection<BatchedStoredTestEvent> getTestEvents()
	{
		return Collections.unmodifiableCollection(events.values());
	}
	
	/**
	 * @return collection of root test events stored in the batch
	 */
	public Collection<BatchedStoredTestEvent> getRootTestEvents()
	{
		return Collections.unmodifiableCollection(rootEvents);
	}
	
	/**
	 * @return true if no test events were added to batch yet
	 */
	public boolean isEmpty()
	{
		return events.isEmpty();
	}
	
	/**
	 * Indicates if the batch cannot hold more test events
	 * @return true if batch capacity is reached and the batch must be flushed to Cradle
	 */
	public boolean isFull()
	{
		return events.size() >= MAX_EVENTS_NUMBER || storedEventsSize >= MAX_EVENTS_SIZE;
	}
	
	
	boolean hasChildren(StoredTestEventId parentId)
	{
		return children.containsKey(parentId);
	}
	
	Collection<BatchedStoredTestEvent> getChildren(StoredTestEventId parentId)
	{
		Collection<BatchedStoredTestEvent> result = children.get(parentId);
		return result != null ? Collections.unmodifiableCollection(result) : Collections.emptyList();
	}
	
	/**
	 * Adds test event to the batch. Batch will verify the event to match batch conditions.
	 * Events can be added to batch until {@link #isFull()} returns true.
	 * Result of this method should be used for all further operations on the event
	 * @param event to add to the batch
	 * @return immutable test event object
	 * @throws CradleStorageException if test event cannot be added to the batch due to verification failure or if batch limit is reached
	 */
	//This method is public and shows that only TestEventToStore can be directly added to batch
	public BatchedStoredTestEvent addTestEvent(TestEventToStore event) throws CradleStorageException
	{
		return addStoredTestEvent(event);
	}
	
	
	private BatchedStoredTestEvent addStoredTestEvent(StoredTestEventWithContent event) throws CradleStorageException
	{
		if (isFull())
			throw new CradleStorageException("Batch is full");
		
		TestEventUtils.validateTestEvent(event, true);
		
		if (events.containsKey(event.getId()))
			throw new CradleStorageException("Test event with ID '"+event.getId()+"' is already present in batch");
		
		StoredTestEventId parentId = event.getParentId();
		if (parentId == null)
			throw new CradleStorageException("Event being added to batch must have a parent. "
					+ "It can be parent of the batch itself or another event already stored in this batch");
		
		boolean isRoot;
		if (parentId.equals(getParentId()))  //Event references batch's parent, so event is actually the root one among events stored in this batch
			isRoot = true;
		else if (!events.containsKey(parentId))
		{
			throw new CradleStorageException("Test event with ID '"+parentId+"' should be parent of the batch itself or "
					+ "should be stored in this batch to be referenced as a parent");
		}
		else
			isRoot = false;
		
		updateBatchData(event);
		
		BatchedStoredTestEvent result = new BatchedStoredTestEvent(event, this);
		events.put(result.getId(), result);
		if (!isRoot)
			children.computeIfAbsent(parentId, k -> new ArrayList<>()).add(result);
		else
			rootEvents.add(result);
		
		if (event.getContent() != null)
			storedEventsSize += event.getContent().length;
		return result;
	}
	
	private void updateBatchData(StoredTestEventWithContent event)
	{
		Instant eventStart = event.getStartTimestamp();
		if (eventStart != null)
		{
			if (startTimestamp == null || startTimestamp.isAfter(eventStart))
				startTimestamp = eventStart;
		}
		
		Instant eventEnd = event.getEndTimestamp();
		if (eventEnd != null)
		{
			if (endTimestamp == null || endTimestamp.isBefore(eventEnd))
				endTimestamp = eventEnd;
		}
		
		if (!event.isSuccess())
			success = false;
	}
}
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.ArrayUtils;

import com.exactpro.cradle.messages.StoredMessageId;
import com.exactpro.cradle.utils.CradleStorageException;
import com.exactpro.cradle.utils.TestEventUtils;

/**
 * Holds information about batch of test events prepared to be stored in Cradle
 * Events stored in the batch can refer to each other to form a hierarchy. No references to these events are possible outside of the batch and vice versa.
 * Root events in the batch should reference batch's parent.
 */
public class TestEventBatchToStore extends TestEventToStore implements TestEventBatch
{
	private final Map<StoredTestEventId, BatchedStoredTestEvent> events = new LinkedHashMap<>();
	private final Collection<BatchedStoredTestEvent> rootEvents = new ArrayList<>();
	private final Map<StoredTestEventId, Collection<BatchedStoredTestEvent>> children = new HashMap<>();
	private final Map<StoredTestEventId, Set<StoredMessageId>> messages = new HashMap<>();
	private final int maxBatchSize;
	private int batchSize = 0;
	
	public TestEventBatchToStore(StoredTestEventId id, String name, StoredTestEventId parentId, int maxBatchSize) throws CradleStorageException
	{
		super(id, name, parentId);
		success = true;
		this.maxBatchSize = maxBatchSize;
	}
	
	
	public static TestEventBatchToStoreBuilder builder(int maxBatchSize)
	{
		return new TestEventBatchToStoreBuilder(maxBatchSize);
	}
	
	
	@Override
	public Set<StoredMessageId> getMessages()
	{
		if (messages == null)  //This is the case when validateTestEvent() is called from super constructor
			return null;
		Set<StoredMessageId> result = new HashSet<>();
		messages.values().forEach(c -> result.addAll(c));
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
		return Collections.unmodifiableCollection(events.values());
	}
	
	@Override
	public Collection<BatchedStoredTestEvent> getRootTestEvents()
	{
		return Collections.unmodifiableCollection(rootEvents);
	}
	
	@Override
	public Map<StoredTestEventId, Set<StoredMessageId>> getBatchMessages()
	{
		return Collections.unmodifiableMap(messages);
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
	
	
	/**
	 * @return size of events currently stored in the batch
	 */
	public int getBatchSize()
	{
		return batchSize;
	}
	
	/**
	 * Indicates if the batch cannot hold more test events
	 * @return true if batch capacity is reached and the batch must be flushed to Cradle
	 */
	public boolean isFull()
	{
		return batchSize >= maxBatchSize;
	}
	
	/**
	 * Shows how many bytes the batch can hold till its capacity is reached
	 * @return number of bytes the batch can hold
	 */
	public int getSpaceLeft()
	{
		int result = maxBatchSize-batchSize;
		return result > 0 ? result : 0;
	}
	
	/**
	 * Shows if batch has enough space to hold given test event
	 * @param event to check against batch capacity
	 * @return true if batch has enough space to hold given test event
	 */
	public boolean hasSpace(TestEventSingleToStore event)
	{
		byte[] content = event.getContent();
		return ArrayUtils.isEmpty(content) || batchSize+content.length <= maxBatchSize;
	}
	
	
	/**
	 * Adds test event to the batch. Batch will verify the event to match batch conditions.
	 * Result of this method should be used for all further operations on the event
	 * @param event to add to the batch
	 * @return immutable test event object
	 * @throws CradleStorageException if test event cannot be added to the batch due to verification failure
	 */
	public BatchedStoredTestEvent addTestEvent(TestEventSingleToStore event) throws CradleStorageException
	{
		if (!hasSpace(event))
			throw new CradleStorageException("Batch has not enough space to hold given test event");
		
		checkEvent(event);
		
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
		
		BatchedStoredTestEvent result = new BatchedStoredTestEvent(event, this, null);
		events.put(result.getId(), result);
		if (!isRoot)
			children.computeIfAbsent(parentId, k -> new ArrayList<>()).add(result);
		else
			rootEvents.add(result);
		
		if (event.getContent() != null)
			batchSize += event.getContent().length;
		
		return result;
	}
	
	private void updateBatchData(TestEventSingle event) throws CradleStorageException
	{
		Instant eventEnd = event.getEndTimestamp();
		if (eventEnd != null)
		{
			if (endTimestamp == null || endTimestamp.isBefore(eventEnd))
				endTimestamp = eventEnd;
		}
		
		if (!event.isSuccess())
			success = false;
		
		//Not checking messages because event being added is already checked for having the same book as the batch and 
		//event messages are checked for the same book in TestEventUtils.validateTestEvent()
		Set<StoredMessageId> eventMessages = event.getMessages();
		if (eventMessages != null && eventMessages.size() > 0)
			messages.put(event.getId(), Collections.unmodifiableSet(new HashSet<>(eventMessages)));
	}
	
	private void checkEvent(TestEventSingle event) throws CradleStorageException
	{
		if (!getBookId().equals(event.getBookId()))
			throw new CradleStorageException("Batch contains events of book '"+getBookId()+"', "
					+ "but in your event it is '"+event.getBookId()+"'");
		if (!getScope().equals(event.getScope()))
			throw new CradleStorageException("Batch contains events of scope '"+getScope()+"', "
					+ "but in your event it is '"+event.getScope()+"'");
		if (event.getStartTimestamp().isBefore(getStartTimestamp()))
			throw new CradleStorageException("Start timestamp of event being added is before the batch start timestamp");
		
		if (events.containsKey(event.getId()))
			throw new CradleStorageException("Test event with ID '"+event.getId()+"' is already present in batch");
	}
}

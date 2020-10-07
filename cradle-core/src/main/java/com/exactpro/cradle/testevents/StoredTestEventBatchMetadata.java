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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class StoredTestEventBatchMetadata
{
	private final StoredTestEventId id,
			parentId;
	private final Map<StoredTestEventId, BatchedStoredTestEventMetadata> events = new LinkedHashMap<>();
	private final Collection<BatchedStoredTestEventMetadata> rootEvents = new ArrayList<>();
	private final Map<StoredTestEventId, Collection<BatchedStoredTestEventMetadata>> children = new HashMap<>();
	
	public StoredTestEventBatchMetadata(StoredTestEventId id, StoredTestEventId parentId)
	{
		this.id = id;
		this.parentId = parentId;
	}
	
	public StoredTestEventBatchMetadata(StoredTestEventBatch batch)
	{
		this.id = batch.getId();
		this.parentId = batch.getParentId();
		for (BatchedStoredTestEvent rootEvent : batch.getRootTestEvents())
		{
			BatchedStoredTestEventMetadata meta = new BatchedStoredTestEventMetadata(rootEvent, this);
			events.put(meta.getId(), meta);
			rootEvents.add(meta);
			if (rootEvent.hasChildren())
				addChildren(rootEvent);
		}
	}
	
	
	private void addChildren(BatchedStoredTestEvent event)
	{
		for (BatchedStoredTestEvent child : event.getChildren())
		{
			BatchedStoredTestEventMetadata meta = new BatchedStoredTestEventMetadata(child, this);
			events.put(meta.getId(), meta);
			children.computeIfAbsent(meta.getParentId(), k -> new ArrayList<>()).add(meta);
			if (child.hasChildren())
				addChildren(child);
		}
	}
	
	
	public static BatchedStoredTestEventMetadata addTestEventMetadata(StoredTestEvent event, StoredTestEventBatchMetadata batch)
	{
		return batch.addStoredTestEventMetadata(event);
	}
	
	
	/**
	 * Returns metadata of test event stored in the batch by ID
	 * @param id of test event to get metadata from batch
	 * @return metadata of test event for given ID, if it is present in the batch, null otherwise
	 */
	public BatchedStoredTestEventMetadata getTestEvent(StoredTestEventId id)
	{
		return events.get(id);
	}
	
	/**
	 * @return collection of test events metadata stored in the batch
	 */
	public Collection<BatchedStoredTestEventMetadata> getTestEvents()
	{
		return Collections.unmodifiableCollection(events.values());
	}
	
	/**
	 * @return collection of root test events metadata stored in the batch
	 */
	public Collection<BatchedStoredTestEventMetadata> getRootTestEvents()
	{
		return Collections.unmodifiableCollection(rootEvents);
	}
	
	/**
	 * @return true if no test events metadata were added to batch
	 */
	public boolean isEmpty()
	{
		return events.isEmpty();
	}
	
	public StoredTestEventId getId()
	{
		return id;
	}
	
	public StoredTestEventId getParentId()
	{
		return parentId;
	}
	
	
	private BatchedStoredTestEventMetadata addStoredTestEventMetadata(StoredTestEvent event)
	{
		StoredTestEventId parentId = event.getParentId();
		boolean isRoot = parentId.equals(getParentId());  //Event references batch's parent, so event is actually the root one among events stored in this batch
		BatchedStoredTestEventMetadata result = new BatchedStoredTestEventMetadata(event, this);
		events.put(result.getId(), result);
		if (!isRoot)
			children.computeIfAbsent(parentId, k -> new ArrayList<>()).add(result);
		else
			rootEvents.add(result);
		
		return result;
	}


	boolean hasChildren(StoredTestEventId parentId)
	{
		return children.containsKey(parentId);
	}
	
	Collection<BatchedStoredTestEventMetadata> getChildren(StoredTestEventId parentId)
	{
		Collection<BatchedStoredTestEventMetadata> result = children.get(parentId);
		return result != null ? Collections.unmodifiableCollection(result) : Collections.emptyList();
	}
}
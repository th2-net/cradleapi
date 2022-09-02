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

/**
 * Holds information about test event stored in Cradle.
 * Can be a single event or a batch of events.
 * Use {@link #isSingle()} or {@link #isBatch()} to determine how to treat the event.
 * Depending on the result, use {@link #asSingle()} or {@link #asBatch()} to work with event as a single event or as a batch, respectively
 */
public class StoredTestEventWrapper
{
	//Wraps StoredTestEventData and not extends it to not add isSingle(), isBatch(), etc. to children of StoredTestEvent, e.g. StoredTestEventSingle
	private final StoredTestEvent eventData;  
	
	public StoredTestEventWrapper(StoredTestEvent eventData)
	{
		this.eventData = eventData;
	}
	
	
	public StoredTestEventId getId()
	{
		return eventData.getId();
	}
	
	public String getName()
	{
		return eventData.getName();
	}
	
	public String getType()
	{
		return eventData.getType();
	}
	
	public Instant getStartTimestamp()
	{
		return eventData.getStartTimestamp();
	}
	
	public Instant getEndTimestamp()
	{
		return eventData.getEndTimestamp();
	}
	
	public boolean isSuccess()
	{
		return eventData.isSuccess();
	}
	
	public StoredTestEventId getParentId()
	{
		return eventData.getParentId();
	}
	
	
	public boolean isSingle()
	{
		return eventData instanceof StoredTestEventSingle;
	}
	
	public boolean isBatch()
	{
		return eventData instanceof StoredTestEventBatch;
	}
	
	public StoredTestEventSingle asSingle()
	{
		if (isSingle())
			return (StoredTestEventSingle)eventData;
		return null;
	}
	
	public StoredTestEventBatch asBatch()
	{
		if (isBatch())
			return (StoredTestEventBatch)eventData;
		return null;
	}

	public Instant getMaxStartTimestamp () {
		if (isSingle()) {
			return asSingle().getStartTimestamp();
		}

		// It's a batch, no need to check

		StoredTestEventBatch batch = asBatch();

		Instant maxStart = getStartTimestamp();
		for (BatchedStoredTestEvent event : batch.getTestEvents()) {
			if (maxStart == null || maxStart.isBefore(event.getStartTimestamp())) {
				maxStart = event.getStartTimestamp();
			}
		}

		return maxStart;
	}
}

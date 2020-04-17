/******************************************************************************
 * Copyright (c) 2009-2020, Exactpro Systems LLC
 * www.exactpro.com
 * Build Software to Test Software
 *
 * All rights reserved.
 * This is unpublished, licensed software, confidential and proprietary 
 * information which is the property of Exactpro Systems LLC or its licensors.
 ******************************************************************************/

package com.exactpro.cradle.testevents;

import java.util.*;

import com.exactpro.cradle.utils.CradleStorageException;

/**
 * Holds information about batch of test events stored in Cradle.
 * All events stored in the batch are linked to its ID
 * Batch has limited capacity. If batch is full, events can't be added to it and the batch must be flushed to Cradle
 */
public class StoredTestEventBatch
{
	public static int MAX_EVENTS_NUMBER = 10;
	
	private final StoredTestEventBatchId id;
	private int storedTestEventsCount = 0;
	private final StoredTestEvent[] testEvents;
	private StoredTestEventId parentId = null;
	
	public StoredTestEventBatch(StoredTestEventBatchId id)
	{
		this.id = id;
		this.testEvents = new StoredTestEvent[MAX_EVENTS_NUMBER];
	}
	
	
	public static StoredTestEventBatch singleton(StoredTestEvent testEvent) throws CradleStorageException
	{
		StoredTestEventBatch result = new StoredTestEventBatch(testEvent.getId().getBatchId());
		result.addTestEvent(testEvent);
		return result;
	}
	

	public StoredTestEventBatchId getId()
	{
		return id;
	}

	public int getStoredTestEventCount()
	{
		return storedTestEventsCount;
	}

	public StoredTestEvent[] getTestEvents()
	{
		return testEvents;
	}
	
	public List<StoredTestEvent> getTestEventsList()
	{
		List<StoredTestEvent> result = new ArrayList<>();
		for (int i = 0; i < storedTestEventsCount; i++)
			result.add(testEvents[i]);
		return result;
	}
	
	public StoredTestEventId getParentId()
	{
		return parentId;
	}
	
	/**
	 * Adds test event to the batch. If test event ID is not specified, batch will add correct ID by itself, else test event ID will be verified to match batch conditions.
	 * Batch contains test events with the same parent ID.
	 * Test events can be added to batch until {@link #isFull()} returns true. 
	 * @param testEvent to add to batch
	 * @return ID assigned to added test event or ID specified in test event, if present
	 * @throws CradleStorageException if test event cannot be added to batch due to ID verification failure or if batch limit is reached
	 */
	public StoredTestEventId addTestEvent(StoredTestEvent testEvent) throws CradleStorageException
	{
		if (isFull())
			throw new CradleStorageException("Batch is full");
		
		if (storedTestEventsCount > 0)
		{
			if (!Objects.equals(parentId, testEvent.getParentId()))  //parentId can be null and this is valid
				throw new CradleStorageException("Parent ID in test event ("+testEvent.getParentId()+") doesn't match with parent ID of other test events in this batch ("+parentId+")");
		}
		else
			parentId = testEvent.getParentId();
		
		if (testEvent.getId() != null)
		{
			StoredTestEventId eventId = testEvent.getId();
			if (!eventId.getBatchId().equals(id))
				throw new CradleStorageException("Batch ID in test event ("+eventId.getBatchId()+") doesn't match with ID of this batch ("+id+")");
			if (eventId.getIndex() != storedTestEventsCount)
				throw new CradleStorageException("Unexpected test event index - "+eventId.getIndex()+". Expected "+storedTestEventsCount);
		}
		else
			testEvent.setId(new StoredTestEventId(id, storedTestEventsCount));
		
		testEvents[storedTestEventsCount++] = testEvent;
		
		return testEvent.getId();
	}
	
	/**
	 * @return number of test events currently stored in the batch
	 */
	public int getTestEventsCount()
	{
		return storedTestEventsCount;
	}
	
	/**
	 * @return true if no test events were added to the batch yet
	 */
	public boolean isEmpty()
	{
		return storedTestEventsCount == 0;
	}
	
	/**
	 * Indicates if the batch can hold more test events
	 * @return true is batch capacity is not reached yet, else the batch cannot be used to store more test events and must be flushed to Cradle
	 */
	public boolean isFull()
	{
		return storedTestEventsCount >= testEvents.length;
	}
}
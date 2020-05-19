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

import java.time.Instant;

/**
 * Holds information about test event stored in Cradle.
 * Can be a single event or a batch of events.
 * Use {@link #isSingle()} or {@link #isBatch()} to determine how to treat the event.
 * Depending on the result, use {@link #asSingle()} or {@link #asBatch()} to work with event as a single event or as a batch, respectively
 */
public class StoredTestEventWrapper
{
	//Wraps StoredTestEventData and not extends it to not add isSingle(), isBatch(), etc. to children of StoredTestEventData, e.g. SingleStoredTestEvent
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
}
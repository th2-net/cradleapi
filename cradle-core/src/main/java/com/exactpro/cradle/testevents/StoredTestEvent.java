/******************************************************************************
 * Copyright (c) 2009-2020, Exactpro Systems LLC
 * www.exactpro.com
 *	 Build Software to Test Software
 *
 * All rights reserved.
 * This is unpublished, licensed software, confidential and proprietary 
 * information which is the property of Exactpro Systems LLC or its licensors.
 ******************************************************************************/

package com.exactpro.cradle.testevents;

import java.time.Instant;

import com.exactpro.cradle.utils.CradleStorageException;

/**
 * Interface for all stored test events. Provides access only to event meta-data. Event content is specific to event
 */
public interface StoredTestEvent extends MinimalTestEventFields
{
	Instant getStartTimestamp();
	Instant getEndTimestamp();
	boolean isSuccess();
	
	
	public static StoredTestEventSingle newStoredTestEventSingle(TestEventToStore event) throws CradleStorageException
	{
		return new StoredTestEventSingle(event);
	}
	
	public static StoredTestEventBatch newStoredTestEventBatch(MinimalTestEventToStore batchData) throws CradleStorageException
	{
		return new StoredTestEventBatch(batchData);
	}
}
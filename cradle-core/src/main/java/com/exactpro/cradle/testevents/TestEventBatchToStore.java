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

/**
 * This class only makes naming more clear: TestEventBatchToStore is used for batches of events while {@link TestEventToStore} is for single events
 */
public class TestEventBatchToStore extends MinimalTestEventToStore
{
	public static TestEventBatchToStoreBuilder builder()
	{
		return new TestEventBatchToStoreBuilder();
	}
}

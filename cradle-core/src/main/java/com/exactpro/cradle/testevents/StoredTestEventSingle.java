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
 * Holds information about single (individual) test event stored in Cradle
 */
public class StoredTestEventSingle extends MinimalStoredTestEvent implements StoredTestEventWithContent
{
	private final Instant startTimestamp,
			endTimestamp;
	private final boolean success;
	private final byte[] content;
	
	public StoredTestEventSingle(StoredTestEventWithContent event)
	{
		super(event);
		this.startTimestamp = event.getStartTimestamp();
		this.endTimestamp = event.getEndTimestamp();
		this.success = event.isSuccess();
		this.content = event.getContent();
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
	
	@Override
	public byte[] getContent()
	{
		return content;
	}
}
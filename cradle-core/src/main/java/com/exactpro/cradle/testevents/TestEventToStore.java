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
 * Object to hold information about one test event prepared to be stored in Cradle
 */
public class TestEventToStore extends MinimalTestEventToStore implements StoredTestEventWithContent
{
	private Instant startTimestamp,
		endTimestamp;
	private boolean success;
	private byte[] content;
	
	
	public static TestEventToStoreBuilder builder()
	{
		return new TestEventToStoreBuilder();
	}
	
	
	@Override
	public Instant getStartTimestamp()
	{
		return startTimestamp;
	}
	
	public void setStartTimestamp(Instant startTimestamp)
	{
		this.startTimestamp = startTimestamp;
	}
	
	
	@Override
	public Instant getEndTimestamp()
	{
		return endTimestamp;
	}
	
	public void setEndTimestamp(Instant endTimestamp)
	{
		this.endTimestamp = endTimestamp;
	}
	
	
	@Override
	public boolean isSuccess()
	{
		return success;
	}
	
	public void setSuccess(boolean success)
	{
		this.success = success;
	}
	
	
	@Override
	public byte[] getContent()
	{
		return content;
	}
	
	public void setContent(byte[] content)
	{
		this.content = content;
	}
}

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

import com.exactpro.cradle.reports.StoredReportId;

public class StoredTestEventBuilder
{
	private StoredTestEvent event;
	
	public StoredTestEventBuilder()
	{
		event = createStoredTestEvent();
	}
	
	
	protected StoredTestEvent createStoredTestEvent()
	{
		return new StoredTestEvent();
	}
	
	private void initIfNeeded()
	{
		if (event == null)
			event = createStoredTestEvent();
	}
	
	
	public StoredTestEventBuilder id(StoredTestEventId id)
	{
		initIfNeeded();
		event.setId(id);
		return this;
	}
	
	public StoredTestEventBuilder name(String name)
	{
		initIfNeeded();
		event.setName(name);
		return this;
	}
	
	public StoredTestEventBuilder type(String type)
	{
		initIfNeeded();
		event.setType(type);
		return this;
	}
	
	public StoredTestEventBuilder startTimestamp(Instant timestamp)
	{
		initIfNeeded();
		event.setStartTimestamp(timestamp);
		return this;
	}
	
	public StoredTestEventBuilder endTimestamp(Instant timestamp)
	{
		initIfNeeded();
		event.setEndTimestamp(timestamp);
		return this;
	}
	
	public StoredTestEventBuilder success(boolean success)
	{
		initIfNeeded();
		event.setSuccess(success);
		return this;
	}
	
	public StoredTestEventBuilder successful()
	{
		initIfNeeded();
		event.setSuccess(true);
		return this;
	}
	
	public StoredTestEventBuilder failed()
	{
		initIfNeeded();
		event.setSuccess(false);
		return this;
	}
	
	public StoredTestEventBuilder content(byte[] content)
	{
		initIfNeeded();
		event.setContent(content);
		return this;
	}
	
	public StoredTestEventBuilder parent(StoredTestEventId id)
	{
		initIfNeeded();
		event.setParentId(id);
		return this;
	}
	
	public StoredTestEventBuilder report(StoredReportId id)
	{
		initIfNeeded();
		event.setReportId(id);
		return this;
	}
	
	
	public StoredTestEvent build()
	{
		initIfNeeded();
		StoredTestEvent result = event;
		event = null;
		return result;
	}
}
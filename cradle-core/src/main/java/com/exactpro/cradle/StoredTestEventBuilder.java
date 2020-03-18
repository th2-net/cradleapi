/******************************************************************************
 * Copyright (c) 2009-2020, Exactpro Systems LLC
 * www.exactpro.com
 * Build Software to Test Software
 *
 * All rights reserved.
 * This is unpublished, licensed software, confidential and proprietary 
 * information which is the property of Exactpro Systems LLC or its licensors.
 ******************************************************************************/

package com.exactpro.cradle;

import java.time.Instant;

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
	
	
	public StoredTestEventBuilder id(String id)
	{
		event.setId(id);
		return this;
	}
	
	public StoredTestEventBuilder name(String name)
	{
		event.setName(name);
		return this;
	}
	
	public StoredTestEventBuilder type(String type)
	{
		event.setType(type);
		return this;
	}
	
	public StoredTestEventBuilder startTimestamp(Instant timestamp)
	{
		event.setStartTimestamp(timestamp);
		return this;
	}
	
	public StoredTestEventBuilder endTimestamp(Instant timestamp)
	{
		event.setEndTimestamp(timestamp);
		return this;
	}
	
	public StoredTestEventBuilder success(boolean success)
	{
		event.setSuccess(success);
		return this;
	}
	
	public StoredTestEventBuilder successful()
	{
		event.setSuccess(true);
		return this;
	}
	
	public StoredTestEventBuilder failed()
	{
		event.setSuccess(false);
		return this;
	}
	
	public StoredTestEventBuilder content(byte[] content)
	{
		event.setContent(content);
		return this;
	}
	
	public StoredTestEventBuilder parent(String id)
	{
		event.setParentId(id);
		return this;
	}
	
	public StoredTestEventBuilder report(String id)
	{
		event.setReportId(id);
		return this;
	}
	
	
	public StoredTestEvent build()
	{
		StoredTestEvent result = event;
		event = createStoredTestEvent();
		return result;
	}
}
/******************************************************************************
 * Copyright (c) 2009-2020, Exactpro Systems LLC
 * www.exactpro.com
 * Build Software to Test Software
 *
 * All rights reserved.
 * This is unpublished, licensed software, confidential and proprietary 
 * information which is the property of Exactpro Systems LLC or its licensors.
 ******************************************************************************/

package com.exactpro.cradle.messages;

import java.time.Instant;

import com.exactpro.cradle.filters.FilterByField;

public class StoredMessageFilter
{
	private FilterByField<String> streamName;
	private FilterByField<Instant> timestampFrom,
			timestampTo;
	
	
	public FilterByField<String> getStreamName()
	{
		return streamName;
	}
	
	public void setStreamName(FilterByField<String> streamName)
	{
		this.streamName = streamName;
	}
	
	
	public FilterByField<Instant> getTimestampFrom()
	{
		return timestampFrom;
	}
	
	public void setTimestampFrom(FilterByField<Instant> timestampFrom)
	{
		this.timestampFrom = timestampFrom;
	}
	
	
	public FilterByField<Instant> getTimestampTo()
	{
		return timestampTo;
	}
	
	public void setTimestampTo(FilterByField<Instant> timestampTo)
	{
		this.timestampTo = timestampTo;
	}
}

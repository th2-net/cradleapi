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

import com.exactpro.cradle.Direction;
import com.exactpro.cradle.filters.FilterByField;

public class StoredMessageFilter
{
	private FilterByField<String> streamName;
	private FilterByField<Direction> direction;
	private FilterByField<Long> index;
	private FilterByField<Instant> timestampFrom,
			timestampTo;
	private int limit;
	
	
	public StoredMessageFilter()
	{
	}
	
	public StoredMessageFilter(StoredMessageFilter copyFrom)
	{
		this.streamName = copyFrom.getStreamName();
		this.direction = copyFrom.getDirection();
		this.index = copyFrom.getIndex();
		this.timestampFrom = copyFrom.getTimestampFrom();
		this.timestampTo = copyFrom.getTimestampTo();
		this.limit = copyFrom.getLimit();
	}
	
	
	public FilterByField<String> getStreamName()
	{
		return streamName;
	}
	
	public void setStreamName(FilterByField<String> streamName)
	{
		this.streamName = streamName;
	}
	
	
	public FilterByField<Direction> getDirection()
	{
		return direction;
	}
	
	public void setDirection(FilterByField<Direction> direction)
	{
		this.direction = direction;
	}
	
	
	public FilterByField<Long> getIndex()
	{
		return index;
	}
	
	public void setIndex(FilterByField<Long> index)
	{
		this.index = index;
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
	
	
	public int getLimit()
	{
		return limit;
	}
	
	public void setLimit(int limit)
	{
		this.limit = limit;
	}
}
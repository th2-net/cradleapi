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
import com.exactpro.cradle.filters.ComparisonOperation;
import com.exactpro.cradle.filters.FilterByFieldBuilder;
import com.exactpro.cradle.filters.FilterForAny;
import com.exactpro.cradle.filters.FilterForAnyBuilder;
import com.exactpro.cradle.filters.FilterForEquals;
import com.exactpro.cradle.filters.FilterForEqualsBuilder;
import com.exactpro.cradle.filters.FilterForGreater;
import com.exactpro.cradle.filters.FilterForGreaterBuilder;
import com.exactpro.cradle.filters.FilterForLess;
import com.exactpro.cradle.filters.FilterForLessBuilder;

/**
 * Builder of filter for stored messages.
 * Various combinations of filter conditions may have different performance because some operations are done on client side.
 */
public class StoredMessageFilterBuilder
{
	private StoredMessageFilter msgFilter;
	
	public FilterForEqualsBuilder<String, StoredMessageFilterBuilder> streamName()
	{
		initIfNeeded();
		FilterForEquals<String> f = new FilterForEquals<>();
		msgFilter.setStreamName(f);
		return new FilterForEqualsBuilder<String, StoredMessageFilterBuilder>(f, this);
	}
	
	public FilterForEqualsBuilder<Direction, StoredMessageFilterBuilder> direction()
	{
		initIfNeeded();
		FilterForEquals<Direction> f = new FilterForEquals<>();
		msgFilter.setDirection(f);
		return new FilterForEqualsBuilder<Direction, StoredMessageFilterBuilder>(f, this);
	}
	
	public FilterForAnyBuilder<Long, StoredMessageFilterBuilder> index()
	{
		initIfNeeded();
		FilterForAny<Long> f = new FilterForAny<>();
		msgFilter.setIndex(f);
		return new FilterForAnyBuilder<Long, StoredMessageFilterBuilder>(f, this);
	}
	
	public FilterForGreaterBuilder<Instant, StoredMessageFilterBuilder> timestampFrom()
	{
		initIfNeeded();
		FilterForGreater<Instant> f = new FilterForGreater<>();
		msgFilter.setTimestampFrom(f);
		return new FilterForGreaterBuilder<Instant, StoredMessageFilterBuilder>(f, this);
	}
	
	public FilterForLessBuilder<Instant, StoredMessageFilterBuilder> timestampTo()
	{
		initIfNeeded();
		FilterForLess<Instant> f = new FilterForLess<>();
		msgFilter.setTimestampTo(f);
		return new FilterForLessBuilder<Instant, StoredMessageFilterBuilder>(f, this);
	}
	
	public StoredMessageFilterBuilder next(StoredMessageId fromId, int count)
	{
		initIfNeeded();
		msgFilter.setStreamName(new FilterForEquals<String>(fromId.getStreamName()));
		msgFilter.setDirection(new FilterForEquals<Direction>(fromId.getDirection()));
		msgFilter.setIndex(new FilterForAny<Long>(fromId.getIndex(), ComparisonOperation.GREATER));
		msgFilter.setLimit(count);
		return this;
	}
	
	public StoredMessageFilterBuilder previous(StoredMessageId fromId, int count)
	{
		initIfNeeded();
		msgFilter.setStreamName(new FilterForEquals<String>(fromId.getStreamName()));
		msgFilter.setDirection(new FilterForEquals<Direction>(fromId.getDirection()));
		msgFilter.setIndex(new FilterForAny<Long>(fromId.getIndex(), ComparisonOperation.LESS));
		msgFilter.setLimit(count);
		return this;
	}
	
	/**
	 * Sets maximum number of messages to get after filtering
	 * @param limit max number of message to return
	 * @return the same builder instance to continue building chain
	 */
	public StoredMessageFilterBuilder limit(int limit)
	{
		msgFilter.setLimit(limit);
		return this;
	}
	
	public StoredMessageFilter build()
	{
		initIfNeeded();
		StoredMessageFilter result = msgFilter;
		msgFilter = null;
		return result;
	}
	
	
	private void initIfNeeded()
	{
		if (msgFilter == null)
			msgFilter = createStoredMessageFilter();
	}
	
	protected StoredMessageFilter createStoredMessageFilter()
	{
		return new StoredMessageFilter();
	}
}

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
import com.exactpro.cradle.filters.FilterByFieldBuilder;

/**
 * Builder of filter for stored messages.
 * Various combinations of filter conditions may have different performance because some operations are done on client side.
 */
public class StoredMessageFilterBuilder
{
	private StoredMessageFilter msgFilter;
	
	public FilterByFieldBuilder<String, StoredMessageFilterBuilder> streamName()
	{
		initIfNeeded();
		FilterByField<String> f = new FilterByField<>();
		msgFilter.setStreamName(f);
		return new FilterByFieldBuilder<String, StoredMessageFilterBuilder>(f, this);
	}
	
	public FilterByFieldBuilder<Direction, StoredMessageFilterBuilder> direction()
	{
		initIfNeeded();
		FilterByField<Direction> f = new FilterByField<>();
		msgFilter.setDirection(f);
		return new FilterByFieldBuilder<Direction, StoredMessageFilterBuilder>(f, this);
	}
	
	public FilterByFieldBuilder<Instant, StoredMessageFilterBuilder> timestampFrom()
	{
		initIfNeeded();
		FilterByField<Instant> f = new FilterByField<>();
		msgFilter.setTimestampFrom(f);
		return new FilterByFieldBuilder<Instant, StoredMessageFilterBuilder>(f, this);
	}
	
	public FilterByFieldBuilder<Instant, StoredMessageFilterBuilder> timestampTo()
	{
		initIfNeeded();
		FilterByField<Instant> f = new FilterByField<>();
		msgFilter.setTimestampTo(f);
		return new FilterByFieldBuilder<Instant, StoredMessageFilterBuilder>(f, this);
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

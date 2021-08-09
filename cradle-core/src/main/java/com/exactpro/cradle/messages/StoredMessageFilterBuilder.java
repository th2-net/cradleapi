/*
 * Copyright 2020-2021 Exactpro (Exactpro Systems Limited)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.exactpro.cradle.messages;

import java.time.Instant;

import com.exactpro.cradle.Direction;
import com.exactpro.cradle.Order;
import com.exactpro.cradle.PageId;
import com.exactpro.cradle.filters.ComparisonOperation;
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
	private final PageId pageId;
	private StoredMessageFilter msgFilter;
	
	public StoredMessageFilterBuilder(PageId pageId)
	{
		this.pageId = pageId;
	}
	
	
	public FilterForEqualsBuilder<String, StoredMessageFilterBuilder> sessionAlias()
	{
		initIfNeeded();
		FilterForEquals<String> f = new FilterForEquals<>();
		msgFilter.setSessionAlias(f);
		return new FilterForEqualsBuilder<String, StoredMessageFilterBuilder>(f, this);
	}
	
	public FilterForEqualsBuilder<Direction, StoredMessageFilterBuilder> direction()
	{
		initIfNeeded();
		FilterForEquals<Direction> f = new FilterForEquals<>();
		msgFilter.setDirection(f);
		return new FilterForEqualsBuilder<Direction, StoredMessageFilterBuilder>(f, this);
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
	
	public FilterForAnyBuilder<Long, StoredMessageFilterBuilder> sequence()
	{
		initIfNeeded();
		FilterForAny<Long> f = new FilterForAny<>();
		msgFilter.setSequence(f);
		return new FilterForAnyBuilder<Long, StoredMessageFilterBuilder>(f, this);
	}
	
	public StoredMessageFilterBuilder next(StoredMessageId fromId, int count)
	{
		initIfNeeded();
		msgFilter.setSessionAlias(new FilterForEquals<String>(fromId.getSessionAlias()));
		msgFilter.setDirection(new FilterForEquals<Direction>(fromId.getDirection()));
		msgFilter.setTimestampFrom(new FilterForGreater<Instant>(fromId.getTimestamp()));
		msgFilter.setTimestampTo(null);
		msgFilter.setSequence(new FilterForAny<Long>(fromId.getSequence(), ComparisonOperation.GREATER));
		msgFilter.setLimit(count);
		return this;
	}
	
	public StoredMessageFilterBuilder previous(StoredMessageId fromId, int count)
	{
		initIfNeeded();
		msgFilter.setSessionAlias(new FilterForEquals<String>(fromId.getSessionAlias()));
		msgFilter.setDirection(new FilterForEquals<Direction>(fromId.getDirection()));
		msgFilter.setTimestampFrom(new FilterForGreater<Instant>(fromId.getTimestamp()));
		msgFilter.setTimestampTo(null);
		msgFilter.setSequence(new FilterForAny<Long>(fromId.getSequence(), ComparisonOperation.LESS));
		msgFilter.setLimit(count);
		return this;
	}
	
	/**
	 * Sets maximum number of messages to get after filtering
	 * @param limit max number of messages to return
	 * @return the same builder instance to continue building chain
	 */
	public StoredMessageFilterBuilder limit(int limit)
	{
		initIfNeeded();
		msgFilter.setLimit(limit);
		return this;
	}

	public StoredMessageFilterBuilder order(Order order)
	{
		initIfNeeded();
		msgFilter.setOrder(order);
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
		return new StoredMessageFilter(pageId);
	}
}

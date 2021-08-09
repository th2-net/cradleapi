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
import java.util.ArrayList;
import java.util.List;

import com.exactpro.cradle.Direction;
import com.exactpro.cradle.Order;
import com.exactpro.cradle.PageId;
import com.exactpro.cradle.filters.FilterForAny;
import com.exactpro.cradle.filters.FilterForEquals;
import com.exactpro.cradle.filters.FilterForGreater;
import com.exactpro.cradle.filters.FilterForLess;

public class StoredMessageFilter
{
	private final PageId pageId;
	private FilterForEquals<String> sessionAlias;
	private FilterForEquals<Direction> direction;
	private FilterForGreater<Instant> timestampFrom;
	private FilterForLess<Instant> timestampTo;
	private FilterForAny<Long> sequence;
	private int limit;
	private Order order = Order.DIRECT;
	private long leftBoundIndex = -1;
	
	public StoredMessageFilter(PageId pageId)
	{
		this.pageId = pageId;
	}
	
	public StoredMessageFilter(StoredMessageFilter copyFrom)
	{
		this.pageId = copyFrom.getPageId();
		this.sessionAlias = copyFrom.getSessionAlias();
		this.direction = copyFrom.getDirection();
		this.timestampFrom = copyFrom.getTimestampFrom();
		this.timestampTo = copyFrom.getTimestampTo();
		this.sequence = copyFrom.getSequence();
		this.limit = copyFrom.getLimit();
		this.order = copyFrom.getOrder();
	}
	
	
	public PageId getPageId()
	{
		return pageId;
	}
	
	
	public FilterForEquals<String> getSessionAlias()
	{
		return sessionAlias;
	}
	
	public void setSessionAlias(FilterForEquals<String> sessionAlias)
	{
		this.sessionAlias = sessionAlias;
	}
	
	
	public FilterForEquals<Direction> getDirection()
	{
		return direction;
	}
	
	public void setDirection(FilterForEquals<Direction> direction)
	{
		this.direction = direction;
	}
	
	
	public FilterForGreater<Instant> getTimestampFrom()
	{
		return timestampFrom;
	}
	
	public void setTimestampFrom(FilterForGreater<Instant> timestampFrom)
	{
		this.timestampFrom = timestampFrom;
	}
	
	
	public FilterForLess<Instant> getTimestampTo()
	{
		return timestampTo;
	}
	
	public void setTimestampTo(FilterForLess<Instant> timestampTo)
	{
		this.timestampTo = timestampTo;
	}
	
	
	public FilterForAny<Long> getSequence()
	{
		return sequence;
	}
	
	public void setSequence(FilterForAny<Long> sequence)
	{
		this.sequence = sequence;
	}
	
	
	public int getLimit()
	{
		return limit;
	}
	
	public void setLimit(int limit)
	{
		this.limit = limit;
	}
	
	
	public Order getOrder()
	{
		return order;
	}
	
	public void setOrder(Order order)
	{
		this.order = order == null ? Order.DIRECT : order;
	}
	
	
	/**
	 * @return calculated left bound for message sequence number while filtering by sequence with "is less" or "is less or equals" condition and limit involved
	 * This method is for internal use
	 */
	public long getLeftBoundSequence()
	{
		return leftBoundIndex;
	}
	
	/**
	 * Sets calculated left bound for message sequence number . This method is for internal use
	 * @param leftBoundIndex calculated for filter
	 */
	public void setLeftBoundIndex(long leftBoundIndex)
	{
		this.leftBoundIndex = leftBoundIndex;
	}

	@Override
	public String toString()
	{
		List<String> result = new ArrayList<>(10);
		result.add("pageId=" + pageId);
		if (sessionAlias != null)
			result.add("session alias" + sessionAlias);
		if (direction != null)
			result.add("direction" + direction);
		if (timestampFrom != null)
			result.add("timestamp" + timestampFrom);
		if (timestampTo != null)
			result.add("timestamp" + timestampTo);
		if (sequence != null)
			result.add("sequence" + sequence);
		if (limit > 0)
			result.add("limit=" + limit);
		if (order != null)
			result.add("order=" + order);
		return String.join(", ", result);
	}
}

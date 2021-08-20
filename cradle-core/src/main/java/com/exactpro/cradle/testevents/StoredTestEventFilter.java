/*
 * Copyright 2021-2021 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.cradle.testevents;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import com.exactpro.cradle.Order;
import com.exactpro.cradle.books.PageId;
import com.exactpro.cradle.filters.FilterForGreater;
import com.exactpro.cradle.filters.FilterForLess;

public class StoredTestEventFilter
{
	private FilterForGreater<Instant> timestampFrom;
	private FilterForLess<Instant> timestampTo;
	private StoredTestEventId parentId;
	private int limit;
	private Order order = Order.DIRECT;
	private PageId pageId;
	
	public StoredTestEventFilter()
	{
	}
	
	public StoredTestEventFilter(StoredTestEventFilter copyFrom)
	{
		this.timestampFrom = copyFrom.getTimestampFrom();
		this.timestampTo = copyFrom.getTimestampTo();
		this.parentId = copyFrom.getParentId();
		this.limit = copyFrom.getLimit();
		this.order = copyFrom.getOrder();
		this.pageId = copyFrom.getPageId();
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
	
	
	public StoredTestEventId getParentId()
	{
		return parentId;
	}
	
	public void setParentId(StoredTestEventId parentId)
	{
		this.parentId = parentId;
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
	
	
	public PageId getPageId()
	{
		return pageId;
	}
	
	public void setPageId(PageId pageId)
	{
		this.pageId = pageId;
	}
	
	
	@Override
	public String toString()
	{
		List<String> result = new ArrayList<>(10);
		if (timestampFrom != null)
			result.add("timestamp" + timestampFrom);
		if (timestampTo != null)
			result.add("timestamp" + timestampTo);
		if (parentId != null)
			result.add("parent ID=" + parentId);
		if (limit > 0)
			result.add("limit=" + limit);
		if (order != null)
			result.add("order=" + order);
		if (pageId != null)
			result.add("page=" + pageId);
		return String.join(", ", result);
	}
}

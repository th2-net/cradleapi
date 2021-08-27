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

import com.exactpro.cradle.BookId;
import com.exactpro.cradle.Order;
import com.exactpro.cradle.PageId;
import com.exactpro.cradle.filters.FilterForGreater;
import com.exactpro.cradle.filters.FilterForLess;

public class TestEventFilter
{
	private BookId bookId;
	private String scope;
	private FilterForGreater<Instant> startTimestampFrom;
	private FilterForLess<Instant> startTimestampTo;
	private StoredTestEventId parentId;
	private int limit;
	private Order order = Order.DIRECT;
	private PageId pageId;
	
	public TestEventFilter()
	{
	}
	
	public TestEventFilter(TestEventFilter copyFrom)
	{
		this.bookId = copyFrom.getBookId();
		this.scope = copyFrom.getScope();
		this.startTimestampFrom = copyFrom.getStartTimestampFrom();
		this.startTimestampTo = copyFrom.getStartTimestampTo();
		this.parentId = copyFrom.getParentId();
		this.limit = copyFrom.getLimit();
		this.order = copyFrom.getOrder();
		this.pageId = copyFrom.getPageId();
	}
	
	
	public static TestEventFilterBuilder builder()
	{
		return new TestEventFilterBuilder();
	}
	
	
	public BookId getBookId()
	{
		return bookId;
	}
	
	public void setBookId(BookId bookId)
	{
		this.bookId = bookId;
	}
	
	
	public String getScope()
	{
		return scope;
	}
	
	public void setScope(String scope)
	{
		this.scope = scope;
	}
	
	
	public FilterForGreater<Instant> getStartTimestampFrom()
	{
		return startTimestampFrom;
	}
	
	public void setStartTimestampFrom(FilterForGreater<Instant> startTimestampFrom)
	{
		this.startTimestampFrom = startTimestampFrom;
	}
	
	
	public FilterForLess<Instant> getStartTimestampTo()
	{
		return startTimestampTo;
	}
	
	public void setStartTimestampTo(FilterForLess<Instant> startTimestampTo)
	{
		this.startTimestampTo = startTimestampTo;
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
		if (bookId != null)
			result.add("book=" + bookId);
		if (scope != null)
			result.add("scope=" + scope);
		if (startTimestampFrom != null)
			result.add("timestamp" + startTimestampFrom);
		if (startTimestampTo != null)
			result.add("timestamp" + startTimestampTo);
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

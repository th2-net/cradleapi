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

import com.exactpro.cradle.BookId;
import com.exactpro.cradle.Order;
import com.exactpro.cradle.PageId;
import com.exactpro.cradle.filters.FilterForGreater;
import com.exactpro.cradle.filters.FilterForGreaterBuilder;
import com.exactpro.cradle.filters.FilterForLess;
import com.exactpro.cradle.filters.FilterForLessBuilder;
import com.exactpro.cradle.utils.CradleStorageException;

/**
 * Builder of filter for test events.
 * Various combinations of filter conditions may have different performance because some operations are done on client side.
 */
public class TestEventFilterBuilder
{
	private BookId bookId;
	private String scope;
	private PageId pageId;
	private FilterForGreater<Instant> startTimestampFrom;
	private FilterForLess<Instant> startTimestampTo;
	private StoredTestEventId parentId;
	private boolean root;
	private int limit;
	private Order order = Order.DIRECT;
	
	public TestEventFilterBuilder bookId(BookId bookId)
	{
		this.bookId = bookId;
		return this;
	}
	
	public TestEventFilterBuilder scope(String scope)
	{
		this.scope = scope;
		return this;
	}
	
	public TestEventFilterBuilder pageId(PageId pageId)
	{
		this.pageId = pageId;
		return this;
	}
	
	public FilterForGreaterBuilder<Instant, TestEventFilterBuilder> startTimestampFrom()
	{
		FilterForGreater<Instant> f = new FilterForGreater<>();
		startTimestampFrom = f;
		return new FilterForGreaterBuilder<Instant, TestEventFilterBuilder>(f, this);
	}
	
	public FilterForLessBuilder<Instant, TestEventFilterBuilder> startTimestampTo()
	{
		FilterForLess<Instant> f = new FilterForLess<>();
		startTimestampTo = f;
		return new FilterForLessBuilder<Instant, TestEventFilterBuilder>(f, this);
	}
	
	public TestEventFilterBuilder parent(StoredTestEventId parentId)
	{
		this.parentId = parentId;
		this.root = false;
		return this;
	}
	
	public TestEventFilterBuilder root()
	{
		root = true;
		parentId = null;
		return this;
	}
	
	/**
	 * Sets maximum number of test events to get after filtering
	 * @param limit max number of test events to return
	 * @return the same builder instance to continue building chain
	 */
	public TestEventFilterBuilder limit(int limit)
	{
		this.limit = limit;
		return this;
	}

	public TestEventFilterBuilder order(Order order)
	{
		this.order = order;
		return this;
	}
	
	
	public TestEventFilter build() throws CradleStorageException
	{
		try
		{
			TestEventFilter result = createStoredTestEventFilter();
			result.setStartTimestampFrom(startTimestampFrom);
			result.setStartTimestampTo(startTimestampTo);
			
			if (root)
				result.setRoot();
			else
				result.setParentId(parentId);
			
			result.setLimit(limit);
			result.setOrder(order);
			return result;
		}
		finally
		{
			reset();
		}
	}
	
	
	protected TestEventFilter createStoredTestEventFilter() throws CradleStorageException
	{
		return new TestEventFilter(bookId, scope, pageId);
	}
	
	protected void reset()
	{
		bookId = null;
		scope = null;
		pageId = null;
		startTimestampFrom = null;
		startTimestampTo = null;
		parentId = null;
		root = false;
		limit = 0;
		order = Order.DIRECT;
	}
}

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

/**
 * Builder of filter for test events.
 * Various combinations of filter conditions may have different performance because some operations are done on client side.
 */
public class TestEventFilterBuilder
{
	private TestEventFilter eventFilter;
	
	public TestEventFilterBuilder bookId(BookId bookId)
	{
		initIfNeeded();
		eventFilter.setBookId(bookId);
		return this;
	}
	
	public TestEventFilterBuilder scope(String scope)
	{
		initIfNeeded();
		eventFilter.setScope(scope);
		return this;
	}
	
	public FilterForGreaterBuilder<Instant, TestEventFilterBuilder> startTimestampFrom()
	{
		initIfNeeded();
		FilterForGreater<Instant> f = new FilterForGreater<>();
		eventFilter.setStartTimestampFrom(f);
		return new FilterForGreaterBuilder<Instant, TestEventFilterBuilder>(f, this);
	}
	
	public FilterForLessBuilder<Instant, TestEventFilterBuilder> startTimestampTo()
	{
		initIfNeeded();
		FilterForLess<Instant> f = new FilterForLess<>();
		eventFilter.setStartTimestampTo(f);
		return new FilterForLessBuilder<Instant, TestEventFilterBuilder>(f, this);
	}
	
	public TestEventFilterBuilder parent(StoredTestEventId parentId)
	{
		initIfNeeded();
		eventFilter.setParentId(parentId);
		return this;
	}
	
	public TestEventFilterBuilder root()
	{
		initIfNeeded();
		eventFilter.setRoot();
		return this;
	}
	
	/**
	 * Sets maximum number of test events to get after filtering
	 * @param limit max number of test events to return
	 * @return the same builder instance to continue building chain
	 */
	public TestEventFilterBuilder limit(int limit)
	{
		initIfNeeded();
		eventFilter.setLimit(limit);
		return this;
	}

	public TestEventFilterBuilder order(Order order)
	{
		initIfNeeded();
		eventFilter.setOrder(order);
		return this;
	}
	
	public TestEventFilterBuilder pageId(PageId pageId)
	{
		initIfNeeded();
		eventFilter.setPageId(pageId);
		return this;
	}
	
	public TestEventFilter build()
	{
		initIfNeeded();
		TestEventFilter result = eventFilter;
		eventFilter = null;
		return result;
	}
	
	
	private void initIfNeeded()
	{
		if (eventFilter == null)
			eventFilter = createStoredTestEventFilter();
	}
	
	protected TestEventFilter createStoredTestEventFilter()
	{
		return new TestEventFilter();
	}
}

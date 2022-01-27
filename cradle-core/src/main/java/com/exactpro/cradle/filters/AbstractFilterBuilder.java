/*
 * Copyright 2022-2022 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.cradle.filters;

import com.exactpro.cradle.BookId;
import com.exactpro.cradle.FetchParameters;
import com.exactpro.cradle.Order;
import com.exactpro.cradle.PageId;
import com.exactpro.cradle.utils.CradleStorageException;

import java.time.Instant;

public abstract class AbstractFilterBuilder<T extends AbstractFilterBuilder<T,R>, R extends AbstractFilter>
{
	private BookId bookId;
	private PageId pageId;
	private FetchParameters fetchParameters;
	private FilterForGreater<Instant> timestampFrom;
	private FilterForLess<Instant> timestampTo;
	private int limit;
	private Order order = Order.DIRECT;

	public BookId getBookId()
	{
		return bookId;
	}

	public PageId getPageId()
	{
		return pageId;
	}

	public FetchParameters getFetchParameters()
	{
		return fetchParameters;
	}

	public FilterForGreater<Instant> getTimestampFrom()
	{
		return timestampFrom;
	}

	public FilterForLess<Instant> getTimestampTo()
	{
		return timestampTo;
	}

	public int getLimit()
	{
		return limit;
	}

	public Order getOrder()
	{
		return order;
	}

	@SuppressWarnings("unchecked")
	public T bookId(BookId bookId)
	{
		this.bookId = bookId;
		return (T) this;
	}

	@SuppressWarnings("unchecked")
	public T pageId(PageId pageId)
	{
		this.pageId = pageId;
		return (T) this;
	}

	@SuppressWarnings("unchecked")
	public T fetchParameters(FetchParameters fetchParameters)
	{
		this.fetchParameters = fetchParameters;
		return (T) this;
	}

	@SuppressWarnings("unchecked")
	public FilterForGreaterBuilder<Instant, T> timestampFrom()
	{
		FilterForGreater<Instant> f = new FilterForGreater<>();
		timestampFrom = f;
		return new FilterForGreaterBuilder<>(f, (T) this);
	}

	@SuppressWarnings("unchecked")
	public FilterForLessBuilder<Instant, T> timestampTo()
	{
		FilterForLess<Instant> f = new FilterForLess<>();
		timestampTo = f;
		return new FilterForLessBuilder<>(f, (T) this);
	}

	/**
	 * Sets maximum number of messages to get after filtering
	 * @param limit max number of messages to return
	 * @return the same builder instance to continue building chain
	 */
	@SuppressWarnings("unchecked")
	public T limit(int limit)
	{
		this.limit = limit;
		return (T) this;
	}

	@SuppressWarnings("unchecked")
	public T order(Order order)
	{
		this.order = order;
		return (T) this;
	}

	protected void reset()
	{
		bookId = null;
		pageId = null;
		fetchParameters = null;
		timestampFrom = null;
		timestampTo = null;
		limit = 0;
		order = Order.DIRECT;
	}

	public R build() throws CradleStorageException
	{
		R instance = getFilterInstance();
		instance.setFetchParameters(fetchParameters);
		instance.setFrom(timestampFrom);
		instance.setTo(timestampTo);
		instance.setLimit(limit);
		instance.setOrder(order);

		return instance;
	}

	abstract protected R getFilterInstance() throws CradleStorageException;
}

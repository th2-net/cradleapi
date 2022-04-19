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

public abstract class AbstractFilter
{
	protected static final String TO_STRING_DELIMITER = ", ";

	private final BookId bookId;
	private PageId pageId;
	private FetchParameters fetchParameters;
	private FilterForGreater<Instant> from;
	private FilterForLess<Instant> to;
	private int limit;
	private Order order = Order.DIRECT;

	protected AbstractFilter(BookId bookId)
	{
		this.bookId = bookId;
	}
	
	protected AbstractFilter(BookId bookId, PageId pageId)
	{
		this(bookId);
		this.pageId = pageId;
	}

	protected AbstractFilter(AbstractFilter copyFrom)
	{
		this.bookId = copyFrom.getBookId();
		this.pageId = copyFrom.getPageId();
		this.from = copyFrom.getFrom();
		this.to = copyFrom.getTo();
		this.limit = copyFrom.getLimit();
		this.order = copyFrom.getOrder();
		this.fetchParameters = copyFrom.getFetchParameters();
	}

	public BookId getBookId()
	{
		return bookId;
	}

	public PageId getPageId()
	{
		return pageId;
	}

	public void setPageId(PageId pageId)
	{
		this.pageId = pageId;
	}

	protected FilterForGreater<Instant> getFrom()
	{
		return from;
	}

	protected void setFrom(FilterForGreater<Instant> from)
	{
		this.from = from;
	}


	protected FilterForLess<Instant> getTo()
	{
		return to;
	}

	protected void setTo(FilterForLess<Instant> to)
	{
		this.to = to;
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


	public FetchParameters getFetchParameters()
	{
		return fetchParameters;
	}

	public void setFetchParameters(FetchParameters fetchParameters)
	{
		this.fetchParameters = fetchParameters;
	}

	protected void validate() throws CradleStorageException
	{
		if (bookId == null)
			throw new CradleStorageException("bookId is mandatory");
		if (pageId != null && !pageId.getBookId().equals(bookId))
			throw new CradleStorageException("pageId must be from book '"+bookId+"'");
	}
}

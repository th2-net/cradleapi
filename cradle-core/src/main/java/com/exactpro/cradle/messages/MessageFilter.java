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

import org.apache.commons.lang3.StringUtils;

import com.exactpro.cradle.*;
import com.exactpro.cradle.filters.*;
import com.exactpro.cradle.utils.CradleStorageException;

public class MessageFilter
{
	private final BookId bookId;
	private final String sessionAlias;
	private final Direction direction;
	private final PageId pageId;
	private FilterForGreater<Instant> timestampFrom;
	private FilterForLess<Instant> timestampTo;
	private FilterForAny<Long> sequence;
	private int limit;
	private Order order = Order.DIRECT;
	
	public MessageFilter(BookId bookId, String sessionAlias, Direction direction, PageId pageId) throws CradleStorageException
	{
		this.bookId = bookId;
		this.sessionAlias = sessionAlias;
		this.direction = direction;
		this.pageId = pageId;
		validate();
	}
	
	public MessageFilter(BookId bookId, String sessionAlias, Direction direction) throws CradleStorageException
	{
		this(bookId, sessionAlias, direction, null);
	}

	public MessageFilter(MessageFilter copyFrom) throws CradleStorageException
	{
		this.bookId = copyFrom.getBookId();
		this.sessionAlias = copyFrom.getSessionAlias();
		this.direction = copyFrom.getDirection();
		this.timestampFrom = copyFrom.getTimestampFrom();
		this.timestampTo = copyFrom.getTimestampTo();
		this.sequence = copyFrom.getSequence();
		this.limit = copyFrom.getLimit();
		this.order = copyFrom.getOrder();
		this.pageId = copyFrom.getPageId();
		validate();
	}
	
	
	public static MessageFilterBuilder builder()
	{
		return new MessageFilterBuilder();
	}
	
	public static MessageFilterBuilder next(StoredMessageId messageId, int limit)
	{
		return MessageFilterBuilder.next(messageId, limit);
	}
	
	public static MessageFilterBuilder previous(StoredMessageId messageId, int limit)
	{
		return MessageFilterBuilder.previous(messageId, limit);
	}

	
	

	public BookId getBookId()
	{
		return bookId;
	}
	
	public String getSessionAlias()
	{
		return sessionAlias;
	}
	
	public Direction getDirection()
	{
		return direction;
	}
	
	public PageId getPageId()
	{
		return pageId;
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
	
	
	@Override
	public String toString()
	{
		List<String> result = new ArrayList<>(10);
		if (bookId != null)
			result.add("bookId=" + bookId);
		if (sessionAlias != null)
			result.add("sessionAlias=" + sessionAlias);
		if (direction != null)
			result.add("direction=" + direction);
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
		if (pageId != null)
			result.add("pageId=" + pageId.getName());
		return String.join(", ", result);
	}
	
	
	private void validate() throws CradleStorageException
	{
		if (bookId == null)
			throw new CradleStorageException("bookId is mandatory");
		if (StringUtils.isEmpty(sessionAlias))
			throw new CradleStorageException("sessionAlias is mandatory");
		if (direction == null)
			throw new CradleStorageException("direction is mandatory");
		if (pageId != null && !pageId.getBookId().equals(bookId))
			throw new CradleStorageException("pageId must be from book '"+bookId+"'");
	}
}

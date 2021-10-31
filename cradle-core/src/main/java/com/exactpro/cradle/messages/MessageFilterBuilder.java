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

import com.exactpro.cradle.BookId;
import com.exactpro.cradle.Direction;
import com.exactpro.cradle.Order;
import com.exactpro.cradle.PageId;
import com.exactpro.cradle.filters.FilterForAny;
import com.exactpro.cradle.filters.FilterForAnyBuilder;
import com.exactpro.cradle.filters.FilterForGreater;
import com.exactpro.cradle.filters.FilterForGreaterBuilder;
import com.exactpro.cradle.filters.FilterForLess;
import com.exactpro.cradle.filters.FilterForLessBuilder;
import com.exactpro.cradle.utils.CradleStorageException;

/**
 * Builder of filter for stored messages.
 * Various combinations of filter conditions may have different performance because some operations are done on client side.
 */
public class MessageFilterBuilder
{
	private BookId bookId;
	private String sessionAlias;
	private Direction direction;
	private PageId pageId;
	private FilterForGreater<Instant> timestampFrom;
	private FilterForLess<Instant> timestampTo;
	private FilterForAny<Long> sequence;
	private int limit;
	private Order order = Order.DIRECT;
	
	public MessageFilterBuilder()
	{
	}
	
	public static MessageFilterBuilder next(StoredMessageId messageId, int limit)
	{
		return new MessageFilterBuilder()
				.bookId(messageId.getBookId())
				.sessionAlias(messageId.getSessionAlias())
				.direction(messageId.getDirection())
				.timestampFrom().isGreaterThan(messageId.getTimestamp())
				.sequence().isGreaterThan(messageId.getSequence())
				.limit(limit);
	}
	
	public static MessageFilterBuilder previous(StoredMessageId messageId, int limit)
	{
		return new MessageFilterBuilder()
				.bookId(messageId.getBookId())
				.sessionAlias(messageId.getSessionAlias())
				.direction(messageId.getDirection())
				.timestampTo().isLessThan(messageId.getTimestamp())
				.sequence().isLessThan(messageId.getSequence())
				.limit(limit);
	}
	
	
	public MessageFilterBuilder bookId(BookId bookId)
	{
		this.bookId = bookId;
		return this;
	}
	
	public MessageFilterBuilder sessionAlias(String sessionAlias)
	{
		this.sessionAlias = sessionAlias;
		return this;
	}
	
	public MessageFilterBuilder direction(Direction direction)
	{
		this.direction = direction;
		return this;
	}
	
	public MessageFilterBuilder pageId(PageId pageId)
	{
		this.pageId = pageId;
		return this;
	}
	
	public FilterForGreaterBuilder<Instant, MessageFilterBuilder> timestampFrom()
	{
		FilterForGreater<Instant> f = new FilterForGreater<>();
		timestampFrom = f;
		return new FilterForGreaterBuilder<Instant, MessageFilterBuilder>(f, this);
	}
	
	public FilterForLessBuilder<Instant, MessageFilterBuilder> timestampTo()
	{
		FilterForLess<Instant> f = new FilterForLess<>();
		timestampTo = f;
		return new FilterForLessBuilder<Instant, MessageFilterBuilder>(f, this);
	}
	
	public FilterForAnyBuilder<Long, MessageFilterBuilder> sequence()
	{
		FilterForAny<Long> f = new FilterForAny<>();
		sequence = f;
		return new FilterForAnyBuilder<Long, MessageFilterBuilder>(f, this);
	}
	
	
	/**
	 * Sets maximum number of messages to get after filtering
	 * @param limit max number of messages to return
	 * @return the same builder instance to continue building chain
	 */
	public MessageFilterBuilder limit(int limit)
	{
		this.limit = limit;
		return this;
	}

	public MessageFilterBuilder order(Order order)
	{
		this.order = order;
		return this;
	}
	
	
	public MessageFilter build() throws CradleStorageException
	{
		try
		{
			MessageFilter result = createMessageFilter();
			result.setTimestampFrom(timestampFrom);
			result.setTimestampTo(timestampTo);
			result.setSequence(sequence);
			result.setLimit(limit);
			result.setOrder(order);
			return result;
		}
		finally
		{
			reset();
		}
	}
	
	
	protected MessageFilter createMessageFilter() throws CradleStorageException
	{
		return new MessageFilter(bookId, sessionAlias, direction, pageId);
	}
	
	protected void reset()
	{
		bookId = null;
		sessionAlias = null;
		direction = null;
		pageId = null;
		timestampFrom = null;
		timestampTo = null;
		sequence = null;
		limit = 0;
		order = Order.DIRECT;
	}
}
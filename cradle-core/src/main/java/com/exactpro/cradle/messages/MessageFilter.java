/*
 * Copyright 2020-2022 Exactpro (Exactpro Systems Limited)
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

public class MessageFilter extends AbstractFilter
{
	private final String sessionAlias;
	private final Direction direction;
	private FilterForAny<Long> sequence;

	public MessageFilter(BookId bookId, String sessionAlias, Direction direction, PageId pageId) throws CradleStorageException
	{
		super(bookId, pageId);
		this.sessionAlias = sessionAlias;
		this.direction = direction;
		validate();
	}
	
	public MessageFilter(BookId bookId, String sessionAlias, Direction direction) throws CradleStorageException
	{
		this(bookId, sessionAlias, direction, null);
	}

	public MessageFilter(MessageFilter copyFrom) throws CradleStorageException
	{
		super(copyFrom);
		this.sessionAlias = copyFrom.getSessionAlias();
		this.direction = copyFrom.getDirection();
		this.sequence = copyFrom.getSequence();
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

	public String getSessionAlias()
	{
		return sessionAlias;
	}
	
	public Direction getDirection()
	{
		return direction;
	}

	public FilterForGreater<Instant> getTimestampFrom()
	{
		return super.getFrom();
	}

	public void setTimestampFrom(FilterForGreater<Instant> timestampFrom)
	{
		super.setFrom(timestampFrom);
	}


	public FilterForLess<Instant> getTimestampTo()
	{
		return super.getTo();
	}

	public void setTimestampTo(FilterForLess<Instant> timestampTo)
	{
		super.setTo(timestampTo);
	}

	public FilterForAny<Long> getSequence()
	{
		return sequence;
	}
	
	public void setSequence(FilterForAny<Long> sequence)
	{
		this.sequence = sequence;
	}
	
	@Override
	public String toString()
	{
		StringBuilder sb = new StringBuilder();
		if (getBookId() != null)
			sb.append("bookId=").append(getBookId()).append(", ");
		if (sessionAlias != null)
			sb.append("sessionAlias=").append(sessionAlias).append(", ");
		if (direction != null)
			sb.append("direction=").append(direction).append(", ");
		if (getFrom() != null)
			sb.append("timestamp").append(getFrom()).append(", ");
		if (getTo() != null)
			sb.append("timestamp").append(getTo()).append(", ");
		if (sequence != null)
			sb.append("sequence").append(sequence).append(", ");
		if (getLimit() > 0)
			sb.append("limit=").append(getLimit()).append(", ");
		if (getOrder() != null)
			sb.append("order=").append(getOrder()).append(", ");
		if (getPageId() != null)
			sb.append("pageId=").append(getPageId().getName());
		return sb.toString();
	}
	
	@Override
	protected void validate() throws CradleStorageException
	{
		super.validate();
		if (StringUtils.isEmpty(sessionAlias))
			throw new CradleStorageException("sessionAlias is mandatory");
		if (direction == null)
			throw new CradleStorageException("direction is mandatory");
	}
}

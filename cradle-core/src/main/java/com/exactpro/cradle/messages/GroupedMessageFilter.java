/*
 * Copyright 2021-2022 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.cradle.BookId;
import com.exactpro.cradle.PageId;
import com.exactpro.cradle.filters.AbstractFilter;
import com.exactpro.cradle.filters.FilterForGreater;
import com.exactpro.cradle.filters.FilterForLess;

import java.time.Instant;

public class GroupedMessageFilter extends AbstractFilter
{
	private final String groupName;

	public GroupedMessageFilter(BookId bookId, String groupName)
	{
		super(bookId);
		this.groupName = groupName;
	}

	public GroupedMessageFilter(BookId bookId, PageId pageId, String groupName)
	{
		super(bookId, pageId);
		this.groupName = groupName;
	}

	public GroupedMessageFilter(GroupedMessageFilter copyFrom)
	{
		super(copyFrom);
		this.groupName = copyFrom.groupName;
	}
	
	public static GroupedMessageFilterBuilder builder()
	{
		return new GroupedMessageFilterBuilder();
	}

	@Override
	public void setFrom(FilterForGreater<Instant> from)
	{
		super.setFrom(from);
	}

	@Override
	public void setTo(FilterForLess<Instant> to)
	{
		super.setTo(to);
	}

	public String getGroupName()
	{
		return groupName;
	}

	@Override
	public FilterForGreater<Instant> getFrom()
	{
		return super.getFrom();
	}

	@Override
	public FilterForLess<Instant> getTo()
	{
		return super.getTo();
	}

	@Override
	public String toString()
	{
		StringBuilder sb = new StringBuilder("[");
		if (getBookId() != null)
			sb.append("bookId=").append(getBookId()).append(TO_STRING_DELIMITER);
		if (groupName != null)
			sb.append("groupName=").append(groupName).append(TO_STRING_DELIMITER);
		if (getFrom() != null)
			sb.append("timestamp").append(getFrom()).append(TO_STRING_DELIMITER);
		if (getTo() != null)
			sb.append("timestamp").append(getTo()).append(TO_STRING_DELIMITER);
		if (getLimit() > 0)
			sb.append("limit=").append(getLimit()).append(TO_STRING_DELIMITER);
		if (getOrder() != null)
			sb.append("order=").append(getOrder()).append(TO_STRING_DELIMITER);
		if (getPageId() != null)
			sb.append("pageId=").append(getPageId().getName()).append(TO_STRING_DELIMITER);
		if (sb.length() > 1) //Not only first bracket
			sb.setLength(sb.length() - TO_STRING_DELIMITER.length());
		return sb.append("]").toString();
	}
}

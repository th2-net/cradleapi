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

package com.exactpro.cradle.testevents;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import com.exactpro.cradle.filters.AbstractFilter;
import org.apache.commons.lang3.StringUtils;

import com.exactpro.cradle.BookId;
import com.exactpro.cradle.PageId;
import com.exactpro.cradle.filters.FilterForGreater;
import com.exactpro.cradle.filters.FilterForLess;
import com.exactpro.cradle.utils.CradleStorageException;

public class TestEventFilter extends AbstractFilter
{
	private final String scope;
	private StoredTestEventId parentId;
	private boolean root;

	public TestEventFilter(BookId bookId, String scope, PageId pageId) throws CradleStorageException
	{
		super(bookId, pageId);
		this.scope = scope;
		validate();
	}
	
	public TestEventFilter(BookId bookId, String scope) throws CradleStorageException
	{
		this(bookId, scope, null);
	}
	
	public TestEventFilter(TestEventFilter copyFrom) throws CradleStorageException
	{
		super(copyFrom);
		this.scope = copyFrom.getScope();

		//User can specify parentId or root=true or omit both to get all events, whatever the parent. No way to filter "all non-root events"
		if (copyFrom.isRoot())
			setRoot();
		else
			setParentId(copyFrom.getParentId());
		
		validate();
	}
	
	
	public static TestEventFilterBuilder builder()
	{
		return new TestEventFilterBuilder();
	}
	
	public String getScope()
	{
		return scope;
	}


	public FilterForGreater<Instant> getStartTimestampFrom()
	{
		return super.getFrom();
	}
	
	public void setStartTimestampFrom(FilterForGreater<Instant> startTimestampFrom)
	{
		super.setFrom(startTimestampFrom);
	}
	
	
	public FilterForLess<Instant> getStartTimestampTo()
	{
		return super.getTo();
	}
	
	public void setStartTimestampTo(FilterForLess<Instant> startTimestampTo)
	{
		super.setTo(startTimestampTo);
	}
	
	
	public StoredTestEventId getParentId()
	{
		return parentId;
	}
	
	public void setParentId(StoredTestEventId parentId)
	{
		this.parentId = parentId;
		this.root = false;
	}
	
	
	public boolean isRoot()
	{
		return root;
	}
	
	public void setRoot()
	{
		this.root = true;
		this.parentId = null;
	}
	
	@Override
	public String toString()
	{
		List<String> result = new ArrayList<>(10);
		if (getBookId() != null)
			result.add("book=" + getBookId());
		if (scope != null)
			result.add("scope=" + scope);
		if (getFrom() != null)
			result.add("timestamp" + getFrom());
		if (getTo() != null)
			result.add("timestamp" + getTo());
		if (parentId != null)
			result.add("parent ID=" + parentId);
		if (getLimit() > 0)
			result.add("limit=" + getLimit());
		if (getOrder() != null)
			result.add("order=" + getOrder());
		if (getPageId() != null)
			result.add("page=" + getPageId());
		return String.join(", ", result);
	}
	
	
	protected void validate() throws CradleStorageException
	{
		super.validate();
		if (StringUtils.isEmpty(scope))
			throw new CradleStorageException("scope is mandatory");
	}
}

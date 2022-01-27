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

import com.exactpro.cradle.filters.*;
import com.exactpro.cradle.utils.CradleStorageException;

/**
 * Builder of filter for test events.
 * Various combinations of filter conditions may have different performance because some operations are done on client side.
 */
public class TestEventFilterBuilder extends AbstractFilterBuilder<TestEventFilterBuilder, TestEventFilter>
{
	private String scope;
	private StoredTestEventId parentId;
	private boolean root;

	public TestEventFilterBuilder scope(String scope)
	{
		this.scope = scope;
		return this;
	}
	
	public FilterForGreaterBuilder<Instant, TestEventFilterBuilder> startTimestampFrom()
	{
		return super.timestampFrom();
	}
	
	public FilterForLessBuilder<Instant, TestEventFilterBuilder> startTimestampTo()
	{
		return super.timestampTo();
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

	@Override
	public TestEventFilter build() throws CradleStorageException
	{
		try
		{
			TestEventFilter result = super.build();
			if (root)
				result.setRoot();
			else
				result.setParentId(parentId);
			return result;
		}
		finally
		{
			reset();
		}
	}

	@Override
	protected TestEventFilter createFilterInstance() throws CradleStorageException
	{
		return new TestEventFilter(getBookId(), scope, getPageId());
	}

	protected void reset()
	{
		super.reset();
		scope = null;
		parentId = null;
		root = false;
	}
}

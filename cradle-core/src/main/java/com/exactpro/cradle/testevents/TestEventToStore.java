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

package com.exactpro.cradle.testevents;

import java.time.Instant;

import com.exactpro.cradle.books.BookId;

/**
 * Holds basic information about test event prepared to be stored in Cradle. Events extend this class with additional data
 */
public abstract class TestEventToStore implements BasicTestEvent
{
	private StoredTestEventId id;
	private String name,
			type;
	private StoredTestEventId parentId;
	
	
	@Override
	public StoredTestEventId getId()
	{
		return id;
	}
	
	public void setId(StoredTestEventId id)
	{
		this.id = id;
	}
	
	
	@Override
	public String getName()
	{
		return name;
	}
	
	public void setName(String name)
	{
		this.name = name;
	}
	
	
	@Override
	public String getType()
	{
		return type;
	}
	
	public void setType(String type)
	{
		this.type = type;
	}
	
	
	@Override
	public StoredTestEventId getParentId()
	{
		return parentId;
	}
	
	public void setParentId(StoredTestEventId parentId)
	{
		this.parentId = parentId;
	}
	
	
	@Override
	public final BookId getBookId()
	{
		return TestEvent.bookId(this);
	}
	
	@Override
	public final String getScope()
	{
		return TestEvent.scope(this);
	}
	
	@Override
	public final Instant getStartTimestamp()
	{
		return TestEvent.startTimestamp(this);
	}
}

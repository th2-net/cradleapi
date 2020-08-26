/*
 * Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.cradle.utils.CradleStorageException;

public class MinimalStoredTestEvent implements MinimalTestEventFields
{
	private final StoredTestEventId id;
	private final String name,
			type;
	private final StoredTestEventId parentId;
	
	public MinimalStoredTestEvent(StoredTestEventId id, String name, String type, StoredTestEventId parentId) throws CradleStorageException
	{
		this.id = id;
		this.name = name;
		this.type = type;
		this.parentId = parentId;
		
		if (this.id == null)
			throw new CradleStorageException("Test event ID cannot be null");
		if (this.id.equals(parentId))
			throw new CradleStorageException("Test event cannot reference itself");
	}
	
	public MinimalStoredTestEvent(MinimalTestEventFields event) throws CradleStorageException
	{
		this(event.getId(), event.getName(), event.getType(), event.getParentId());
	}
	
	
	@Override
	public StoredTestEventId getId()
	{
		return id;
	}
	
	@Override
	public String getName()
	{
		return name;
	}
	
	@Override
	public String getType()
	{
		return type;
	}
	
	@Override
	public StoredTestEventId getParentId()
	{
		return parentId;
	}
}

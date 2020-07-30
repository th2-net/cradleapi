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

package com.exactpro.cradle.cassandra.iterators;

import java.util.Iterator;

import com.exactpro.cradle.Direction;
import com.exactpro.cradle.cassandra.dao.messages.TimeMessageEntity;
import com.exactpro.cradle.messages.StoredMessageId;

public class TimeMessagesIterator implements Iterator<StoredMessageId>
{
	private final Iterator<TimeMessageEntity> entitiesIterator;
	private String streamName;
	private Direction direction;
	private StoredMessageId nextId;
	
	public TimeMessagesIterator(Iterator<TimeMessageEntity> entitiesIterator)
	{
		this.entitiesIterator = entitiesIterator;
	}
	
	
	@Override
	public boolean hasNext()
	{
		while (entitiesIterator.hasNext())
		{
			StoredMessageId id = entitiesIterator.next().createMessageId();
			if (!id.getStreamName().equals(streamName) || (id.getDirection() != direction))
			{
				streamName = id.getStreamName();
				direction = id.getDirection();
				nextId = id;
				return true;
			}
		}
		return false;
	}
	
	@Override
	public StoredMessageId next()
	{
		if (nextId == null)  //Maybe, hasNext() wasn't called
		{
			if (!hasNext())
				return null;
		}
		
		StoredMessageId result = nextId;
		nextId = null;
		return result;
	}
}
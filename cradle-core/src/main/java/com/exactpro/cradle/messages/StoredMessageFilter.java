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

package com.exactpro.cradle.messages;

import java.time.Instant;

import com.exactpro.cradle.Direction;
import com.exactpro.cradle.filters.FilterForAny;
import com.exactpro.cradle.filters.FilterForEquals;
import com.exactpro.cradle.filters.FilterForGreater;
import com.exactpro.cradle.filters.FilterForLess;

public class StoredMessageFilter
{
	private FilterForEquals<String> streamName;
	private FilterForEquals<Direction> direction;
	private FilterForAny<Long> index;
	private FilterForGreater<Instant> timestampFrom;
	private FilterForLess<Instant> timestampTo;
	private int limit;
	
	
	public StoredMessageFilter()
	{
	}
	
	public StoredMessageFilter(StoredMessageFilter copyFrom)
	{
		this.streamName = copyFrom.getStreamName();
		this.direction = copyFrom.getDirection();
		this.index = copyFrom.getIndex();
		this.timestampFrom = copyFrom.getTimestampFrom();
		this.timestampTo = copyFrom.getTimestampTo();
		this.limit = copyFrom.getLimit();
	}
	
	
	public FilterForEquals<String> getStreamName()
	{
		return streamName;
	}
	
	public void setStreamName(FilterForEquals<String> streamName)
	{
		this.streamName = streamName;
	}
	
	
	public FilterForEquals<Direction> getDirection()
	{
		return direction;
	}
	
	public void setDirection(FilterForEquals<Direction> direction)
	{
		this.direction = direction;
	}
	
	
	public FilterForAny<Long> getIndex()
	{
		return index;
	}
	
	public void setIndex(FilterForAny<Long> index)
	{
		this.index = index;
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
	
	
	public int getLimit()
	{
		return limit;
	}
	
	public void setLimit(int limit)
	{
		this.limit = limit;
	}
}

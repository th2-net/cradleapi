/*
 * Copyright 2021-2021 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.cradle.cassandra.utils;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.LocalTime;

import com.exactpro.cradle.filters.ComparisonOperation;
import com.exactpro.cradle.filters.FilterForGreater;
import com.exactpro.cradle.filters.FilterForLess;
import com.exactpro.cradle.utils.TimeUtils;

public class TimestampBound
{
	private final LocalDateTime timestamp;
	private final ComparisonOperation operation;
	private final String part;
	
	public TimestampBound(LocalDateTime timestamp, ComparisonOperation operation)
	{
		this.timestamp = timestamp;
		this.operation = operation;
		this.part = CassandraTimeUtils.getPart(timestamp);
	}
	
	public TimestampBound(Instant timestamp, ComparisonOperation operation)
	{
		this(TimeUtils.toLocalTimestamp(timestamp), operation);
	}
	
	
	public LocalDateTime getTimestamp()
	{
		return timestamp;
	}
	
	public ComparisonOperation getOperation()
	{
		return operation;
	}
	
	public String getPart()
	{
		return part;
	}
	
	
	public FilterForLess<LocalTime> toFilterForLess()
	{
		return operation == ComparisonOperation.LESS
				? FilterForLess.forLess(timestamp.toLocalTime())
				: FilterForLess.forLessOrEquals(timestamp.toLocalTime());
	}
	
	public FilterForGreater<LocalTime> toFilterForGreater()
	{
		return operation == ComparisonOperation.GREATER
				? FilterForGreater.forGreater(timestamp.toLocalTime())
				: FilterForGreater.forGreaterOrEquals(timestamp.toLocalTime());
	}
}

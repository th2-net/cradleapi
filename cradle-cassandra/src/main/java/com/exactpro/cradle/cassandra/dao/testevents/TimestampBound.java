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

package com.exactpro.cradle.cassandra.dao.testevents;

import java.time.LocalDateTime;

import com.exactpro.cradle.cassandra.utils.CassandraTimeUtils;
import com.exactpro.cradle.filters.ComparisonOperation;

public class TimestampBound
{
	private final LocalDateTime timestamp;
	private final ComparisonOperation operation;
	private final String part;
	private final boolean useTime;
	
	public TimestampBound(LocalDateTime timestamp, ComparisonOperation operation, boolean useTime)
	{
		this.timestamp = timestamp;
		this.operation = operation;
		this.part = CassandraTimeUtils.getPart(timestamp);
		this.useTime = useTime;
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
	
	public boolean isUseTime()
	{
		return useTime;
	}
}

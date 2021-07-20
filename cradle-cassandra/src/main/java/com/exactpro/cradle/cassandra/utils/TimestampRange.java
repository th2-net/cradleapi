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
import java.time.LocalDate;
import java.time.LocalDateTime;

import com.exactpro.cradle.utils.CradleStorageException;

public class TimestampRange
{
	private final LocalDateTime from,
			to;
	
	public TimestampRange(LocalDateTime from, LocalDateTime to) throws CradleStorageException
	{
		checkBoundaries(from, to);
		this.from = from;
		this.to = to;
	}
	
	public TimestampRange(Instant from, Instant to) throws CradleStorageException
	{
		this(DateTimeUtils.toDateTime(from), DateTimeUtils.toDateTime(to));
	}
	
	
	public LocalDateTime getFrom()
	{
		return from;
	}
	
	public LocalDateTime getTo()
	{
		return to;
	}
	
	
	private void checkBoundaries(LocalDateTime fromDateTime, LocalDateTime toDateTime) throws CradleStorageException
	{
		LocalDate fromDate = fromDateTime.toLocalDate(),
				toDate = toDateTime.toLocalDate();
		if (!fromDate.equals(toDate))
			throw new CradleStorageException("Left and right boundaries should be of the same date, but got '"+fromDateTime+"' and '"+toDateTime+"'");
	}
}

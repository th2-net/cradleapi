/*
 * Copyright 2020-2024 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.cradle.utils;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

import com.exactpro.cradle.CradleStorage;

public class TimeUtils
{
	private static final DateTimeFormatter ID_TIMESTAMP_FORMAT = DateTimeFormatter
			.ofPattern("yyyyMMddHHmmssSSSSSSSSS").withZone(CradleStorage.TIMEZONE_OFFSET);
	
	public static Instant cutNanos(Instant instant)
	{
		return Instant.ofEpochSecond(instant.getEpochSecond());
	}
	
	public static Instant fromLocalTimestamp(LocalDateTime timestamp)
	{
		return timestamp.toInstant(CradleStorage.TIMEZONE_OFFSET);
	}
	
	public static LocalDateTime toLocalTimestamp(Instant instant)
	{
		return LocalDateTime.ofInstant(instant, CradleStorage.TIMEZONE_OFFSET);
	}
	
	
	public static Instant fromIdTimestamp(String timestamp)
	{
		return Instant.from(ID_TIMESTAMP_FORMAT.parse(timestamp));
	}
	
	public static String toIdTimestamp(Instant instant)
	{
		// TODO: test and remove redundant line
//		return ID_TIMESTAMP_FORMAT.format(toLocalTimestamp(instant));
		return ID_TIMESTAMP_FORMAT.format(instant);
	}

	public static Instant toInstant(LocalDate localDate, LocalTime localTime)
	{
		if (localDate == null || localTime == null)
			return null;
		return localTime.atDate(localDate).toInstant(CradleStorage.TIMEZONE_OFFSET);
	}

	public static List<LocalDate> splitByDate(Instant from, Instant to) throws CradleStorageException
	{
		if (from == null)
			throw new CradleStorageException("'from' is mandatory parameter and can't be null");
		
		if (to == null)
			to = Instant.now();
		
		LocalDate fromDate = toLocalTimestamp(from).toLocalDate();
		LocalDate toDate = toLocalTimestamp(to).toLocalDate();

		int days = fromDate.until(toDate).getDays();
		List<LocalDate> result = new ArrayList<>();
		for (int i = 0; i <= days; i++)
			result.add(fromDate.plusDays(i));
		
		return result;
	}
}

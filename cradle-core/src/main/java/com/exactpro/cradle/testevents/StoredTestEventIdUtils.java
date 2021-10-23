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

package com.exactpro.cradle.testevents;

import java.text.ParseException;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.List;

import com.exactpro.cradle.BookId;
import com.exactpro.cradle.utils.CradleIdException;
import com.exactpro.cradle.utils.EscapeUtils;
import com.exactpro.cradle.utils.TimeUtils;

/**
 * Utilities to parse {@link StoredTestEventId} from its string representation which consists of timestamp:uniqueId
 */
public class StoredTestEventIdUtils
{
	public static List<String> splitParts(String id) throws CradleIdException
	{
		List<String> parts;
		try
		{
			parts = EscapeUtils.split(id);
		}
		catch (ParseException e)
		{
			throw new CradleIdException("Could not parse test event ID from string '"+id+"'", e);
		}
		
		if (parts.size() != 4)
			throw new CradleIdException("Test Event ID ("+id+") should contain book ID, scope, timestamp and unique ID "
					+ "delimited with '"+StoredTestEventId.ID_PARTS_DELIMITER+"'");
		return parts;
	}
	
	public static String getId(List<String> parts) throws CradleIdException
	{
		return parts.get(3);
	}
	
	public static Instant getTimestamp(List<String> parts) throws CradleIdException
	{
		String timeString = parts.get(2);
		try
		{
			return TimeUtils.fromIdTimestamp(timeString);
		}
		catch (DateTimeParseException e)
		{
			throw new CradleIdException("Invalid timstamp ("+timeString+") in ID '"+EscapeUtils.join(parts)+"'", e);
		}
	}
	
	public static String getScope(List<String> parts) throws CradleIdException
	{
		return parts.get(1);
	}
	
	public static BookId getBook(List<String> parts)
	{
		return new BookId(parts.get(0));
	}
	
	public static String timestampToString(Instant timestamp)
	{
		return TimeUtils.toIdTimestamp(timestamp);
	}
}

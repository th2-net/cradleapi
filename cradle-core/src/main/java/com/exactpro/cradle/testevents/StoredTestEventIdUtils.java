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

import java.time.Instant;
import java.time.format.DateTimeParseException;

import org.apache.commons.lang3.StringUtils;

import com.exactpro.cradle.BookId;
import com.exactpro.cradle.utils.CradleIdException;
import com.exactpro.cradle.utils.TimeUtils;

/**
 * Utilities to parse {@link StoredTestEventId} from its string representation which consists of timestamp:uniqueId
 */
public class StoredTestEventIdUtils
{
	public static String[] splitParts(String id) throws CradleIdException
	{
		String[] parts = id.split(StoredTestEventId.ID_PARTS_DELIMITER);
		if (parts.length < 3)
			throw new CradleIdException("Test Event ID ("+id+") should contain book ID, timestamp and unique ID "
					+ "delimited with '"+StoredTestEventId.ID_PARTS_DELIMITER+"'");
		return parts;
	}
	
	public static String getId(String[] parts) throws CradleIdException
	{
		return parts[parts.length-1];
	}
	
	public static Instant getTimestamp(String[] parts) throws CradleIdException
	{
		String timeString = parts[parts.length-2];
		try
		
		{	return TimeUtils.fromIdTimestamp(timeString);
		}
		catch (DateTimeParseException e)
		{
			throw new CradleIdException("Invalid timstamp ("+timeString+") in ID '"+restoreId(parts)+"'");
		}
	}
	
	public static BookId getBook(String[] parts)
	{
		return new BookId(parts[0]);
	}
	
	public static String timestampToString(Instant timestamp)
	{
		return TimeUtils.toIdTimestamp(timestamp);
	}
	
	
	private static String restoreId(String[] parts)
	{
		return StringUtils.join(parts, StoredTestEventId.ID_PARTS_DELIMITER);
	}
}

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

package com.exactpro.cradle.messages;

import java.text.ParseException;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.List;

import com.exactpro.cradle.BookId;
import com.exactpro.cradle.Direction;
import com.exactpro.cradle.utils.CradleIdException;
import com.exactpro.cradle.utils.EscapeUtils;
import com.exactpro.cradle.utils.TimeUtils;

/**
 * Utilities to parse {@link StoredMessageId} from its string representation which consists of sessionAlias:direction:timestamp:sequence
 */
public class StoredMessageIdUtils
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
			throw new CradleIdException("Could not parse message ID from string '"+id+"'", e);
		}
		
		if (parts.size() != 5)
			throw new CradleIdException("Message ID ("+id+") should contain book ID, session alias, direction, timestamp and sequence number "
					+ "delimited with '"+StoredMessageId.ID_PARTS_DELIMITER+"'");
		return parts;
	}
	
	public static long getSequence(List<String> parts) throws CradleIdException
	{
		String seqString = parts.get(4);
		try
		{
			return Long.parseLong(seqString);
		}
		catch (NumberFormatException e)
		{
			throw new CradleIdException("Invalid sequence number ("+seqString+") in ID '"+EscapeUtils.join(parts)+"'", e);
		}
	}
	
	public static Instant getTimestamp(List<String> parts) throws CradleIdException
	{
		String timeString = parts.get(3);
		try
		{
			return TimeUtils.fromIdTimestamp(timeString);
		}
		catch (DateTimeParseException e)
		{
			throw new CradleIdException("Invalid timstamp ("+timeString+") in ID '"+EscapeUtils.join(parts)+"'", e);
		}
	}
	
	public static Direction getDirection(List<String> parts) throws CradleIdException
	{
		String directionString = parts.get(2);
		Direction direction = Direction.byLabel(directionString);
		if (direction == null)
			throw new CradleIdException("Invalid direction '"+directionString+"' in ID '"+EscapeUtils.join(parts)+"'");
		return direction;
	}
	
	public static String getSessionAlias(List<String> parts)
	{
		return parts.get(1);
	}
	
	public static BookId getBookId(List<String> parts)
	{
		return new BookId(parts.get(0));
	}
	
	
	public static String timestampToString(Instant timestamp)
	{
		return TimeUtils.toIdTimestamp(timestamp);
	}
}

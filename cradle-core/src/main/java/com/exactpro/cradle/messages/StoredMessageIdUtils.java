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

import java.time.Instant;
import java.time.format.DateTimeParseException;

import org.apache.commons.lang3.StringUtils;

import com.exactpro.cradle.BookId;
import com.exactpro.cradle.Direction;
import com.exactpro.cradle.utils.CradleIdException;
import com.exactpro.cradle.utils.TimeUtils;

/**
 * Utilities to parse {@link StoredMessageId} from its string representation which consists of sessionAlias:direction:timestamp:sequence
 */
public class StoredMessageIdUtils
{
	public static String[] splitParts(String id) throws CradleIdException
	{
		String[] parts = id.split(StoredMessageId.ID_PARTS_DELIMITER);
		if (parts.length < 5)
			throw new CradleIdException("Message ID ("+id+") should contain book ID, session alias, direction, timestamp and sequence number "
					+ "delimited with '"+StoredMessageId.ID_PARTS_DELIMITER+"'");
		return parts;
	}
	
	public static long getSequence(String[] parts) throws CradleIdException
	{
		String seqString = parts[parts.length-1];
		try
		{
			return Long.parseLong(seqString);
		}
		catch (NumberFormatException e)
		{
			throw new CradleIdException("Invalid sequence number ("+seqString+") in ID '"+restoreId(parts)+"'");
		}
	}
	
	public static Instant getTimestamp(String[] parts) throws CradleIdException
	{
		String timeString = parts[parts.length-2];
		try
		{
			return TimeUtils.fromIdTimestamp(timeString);
		}
		catch (DateTimeParseException e)
		{
			throw new CradleIdException("Invalid timstamp ("+timeString+") in ID '"+restoreId(parts)+"'");
		}
	}
	
	public static Direction getDirection(String[] parts) throws CradleIdException
	{
		String directionString = parts[parts.length-3];
		Direction direction = Direction.byLabel(directionString);
		if (direction == null)
			throw new CradleIdException("Invalid direction '"+directionString+"' in ID '"+restoreId(parts)+"'");
		return direction;
	}
	
	public static String getSessionAlias(String[] parts)
	{
		StringBuilder session = new StringBuilder();
		for (int i = 1; i < parts.length-3; i++)
		{
			if (session.length() > 0)
				session = session.append(StoredMessageId.ID_PARTS_DELIMITER);
			session = session.append(parts[i]);
		}
		return session.toString();
	}
	
	public static BookId getBookId(String[] parts)
	{
		return new BookId(parts[0]);
	}
	
	
	public static String timestampToString(Instant timestamp)
	{
		return TimeUtils.toIdTimestamp(timestamp);
	}
	
	
	private static String restoreId(String[] parts)
	{
		return StringUtils.join(parts, StoredMessageId.ID_PARTS_DELIMITER);
	}
}

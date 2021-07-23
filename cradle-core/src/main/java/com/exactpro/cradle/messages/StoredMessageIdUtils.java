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

import org.apache.commons.lang3.StringUtils;

import com.exactpro.cradle.Direction;
import com.exactpro.cradle.exceptions.CradleIdException;

/**
 * Utilities to parse {@link StoredMessageId} and {@link StoredMessageBatchId} from their string representation which consists of streamName:direction:index
 */
public class StoredMessageIdUtils
{
	public static String[] splitParts(String id)
	{
		return id.split(StoredMessageBatchId.IDS_DELIMITER);
	}
	
	public static long getIndex(String[] parts) throws CradleIdException
	{
		String indexString = parts[parts.length-1];
		try
		{
			return Long.parseLong(indexString);
		}
		catch (NumberFormatException e)
		{
			throw new CradleIdException("Invalid message index ("+indexString+") in ID '"+restoreId(parts)+"'");
		}
	}
	
	public static Direction getDirection(String[] parts) throws CradleIdException
	{
		String directionString = parts[parts.length-2];
		Direction direction = Direction.byLabel(directionString);
		if (direction == null)
			throw new CradleIdException("Invalid direction '"+directionString+"' in ID '"+restoreId(parts)+"'");
		return direction;
	}
	
	public static String getStreamName(String[] parts)
	{
		StringBuilder streamName = new StringBuilder();
		for (int i = 0; i < parts.length-2; i++)
		{
			if (streamName.length() > 0)
				streamName = streamName.append(StoredMessageBatchId.IDS_DELIMITER);
			streamName = streamName.append(parts[i]);
		}
		return streamName.toString();
	}
	
	
	private static String restoreId(String[] parts)
	{
		return StringUtils.join(parts, StoredMessageBatchId.IDS_DELIMITER);
	}
}

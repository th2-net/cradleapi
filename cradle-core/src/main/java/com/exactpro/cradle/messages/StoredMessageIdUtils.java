/******************************************************************************
 * Copyright (c) 2009-2020, Exactpro Systems LLC
 * www.exactpro.com
 * Build Software to Test Software
 *
 * All rights reserved.
 * This is unpublished, licensed software, confidential and proprietary 
 * information which is the property of Exactpro Systems LLC or its licensors.
 ******************************************************************************/

package com.exactpro.cradle.messages;

import org.apache.commons.lang3.StringUtils;

import com.exactpro.cradle.Direction;
import com.exactpro.cradle.utils.CradleIdException;

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

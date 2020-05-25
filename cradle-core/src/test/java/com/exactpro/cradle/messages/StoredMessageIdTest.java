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

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.exactpro.cradle.Direction;
import com.exactpro.cradle.utils.CradleIdException;

import static com.exactpro.cradle.messages.StoredMessageBatchId.*;

public class StoredMessageIdTest
{
	private String streamName;
	private Direction direction;
	private long index,
			messageIndex;
	private String stringId;
	
	@BeforeClass
	public void prepare()
	{
		streamName = "Stream1";
		direction = Direction.FIRST;
		index = 100;
		messageIndex = index+3;
		stringId = streamName+IDS_DELIMITER+direction.getLabel()+IDS_DELIMITER+messageIndex;
	}
	
	@DataProvider(name = "ids")
	public Object[][] ids()
	{
		return new Object[][]
				{
					{""},
					{streamName},
					{streamName+IDS_DELIMITER},
					{streamName+IDS_DELIMITER+"XXX"},
					{streamName+IDS_DELIMITER+"XXX"+IDS_DELIMITER},
					{streamName+IDS_DELIMITER+"XXX"+IDS_DELIMITER+"NNN"},
					{streamName+IDS_DELIMITER+"XXX"+IDS_DELIMITER+index},
					{streamName+IDS_DELIMITER+direction.getLabel()},
					{streamName+IDS_DELIMITER+direction.getLabel()+IDS_DELIMITER},
					{streamName+IDS_DELIMITER+direction.getLabel()+IDS_DELIMITER+"NNN"}
				};
	}
	
	
	@Test
	public void idToString()
	{
		StoredMessageId id = new StoredMessageId(streamName, direction, messageIndex);
		Assert.assertEquals(id.toString(), stringId);
	}
	
	@Test
	public void idFromString() throws CradleIdException
	{
		StoredMessageId id = new StoredMessageId(streamName, direction, messageIndex),
				fromString = StoredMessageId.fromString(stringId);
		Assert.assertEquals(fromString, id);
	}
	
	@Test(dataProvider = "ids",	
			expectedExceptions = {CradleIdException.class})
	public void idFromStringChecks(String s) throws CradleIdException
	{
		StoredMessageId.fromString(s);
	}
	
	@Test
	public void correctMessageIndex() throws CradleIdException
	{
		StoredMessageId id = StoredMessageId.fromString(stringId);
		Assert.assertEquals(id.getIndex(), messageIndex);
	}
}
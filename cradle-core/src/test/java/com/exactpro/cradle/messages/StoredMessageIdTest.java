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

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.exactpro.cradle.Direction;
import com.exactpro.cradle.exceptions.CradleIdException;

import static com.exactpro.cradle.messages.StoredMessageBatchId.*;

public class StoredMessageIdTest
{
	private String streamName,
			streamNameWithColon;
	private Direction direction;
	private long index,
			messageIndex;
	private String stringId,
			stringIdWithColon;
	
	@BeforeClass
	public void prepare()
	{
		streamName = "Stream1";
		streamNameWithColon = "10.20.30.40:8080-10:20:30:42:9000";
		direction = Direction.FIRST;
		index = 100;
		messageIndex = index+3;
		stringId = streamName+IDS_DELIMITER+direction.getLabel()+IDS_DELIMITER+messageIndex;
		stringIdWithColon = streamNameWithColon+IDS_DELIMITER+direction.getLabel()+IDS_DELIMITER+messageIndex;
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
	
	@Test
	public void idFromStringWithColon() throws CradleIdException
	{
		StoredMessageId id = new StoredMessageId(streamNameWithColon, direction, messageIndex),
				fromString = StoredMessageId.fromString(stringIdWithColon);
		Assert.assertEquals(fromString, id);
	}
	
	@Test(dataProvider = "ids",	
			expectedExceptions = {CradleIdException.class})
	public void idFromStringChecks(String s) throws CradleIdException
	{
		StoredMessageId.fromString(s);
	}
	
	@Test
	public void correctStreamName() throws CradleIdException
	{
		StoredMessageId id = StoredMessageId.fromString(stringIdWithColon);
		Assert.assertEquals(id.getStreamName(), streamNameWithColon);
	}
	
	@Test
	public void correctMessageIndex() throws CradleIdException
	{
		StoredMessageId id = StoredMessageId.fromString(stringId);
		Assert.assertEquals(id.getIndex(), messageIndex);
	}
}

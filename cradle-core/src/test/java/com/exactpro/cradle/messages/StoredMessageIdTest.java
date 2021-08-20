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
import com.exactpro.cradle.books.BookId;
import com.exactpro.cradle.utils.CradleIdException;

import static com.exactpro.cradle.messages.StoredMessageId.*;

import java.time.Instant;

public class StoredMessageIdTest
{
	private BookId book;
	private String sessionAlias,
			sessionAliasWithColon;
	private Direction direction;
	private Instant timestamp;
	private long seq,
			messageSeq;
	private String stringId,
			stringIdWithColon;
	
	@BeforeClass
	public void prepare()
	{
		book = new BookId("book1");
		sessionAlias = "Session1";
		sessionAliasWithColon = "10.20.30.40:8080-10:20:30:42:9000";
		direction = Direction.FIRST;
		timestamp = Instant.EPOCH;
		seq = 100;
		messageSeq = seq+3;
		stringId = book+ID_PARTS_DELIMITER
				+sessionAlias+ID_PARTS_DELIMITER
				+direction.getLabel()+ID_PARTS_DELIMITER
				+StoredMessageIdUtils.timestampToString(timestamp)+ID_PARTS_DELIMITER
				+messageSeq;
		stringIdWithColon = book+ID_PARTS_DELIMITER
				+sessionAliasWithColon+ID_PARTS_DELIMITER
				+direction.getLabel()+ID_PARTS_DELIMITER
				+StoredMessageIdUtils.timestampToString(timestamp)+ID_PARTS_DELIMITER
				+messageSeq;
	}
	
	@DataProvider(name = "ids")
	public Object[][] ids()
	{
		return new Object[][]
				{
					{""},
					{book.toString()},
					{book+ID_PARTS_DELIMITER},
					{book+ID_PARTS_DELIMITER+sessionAlias},
					{book+ID_PARTS_DELIMITER+sessionAlias+ID_PARTS_DELIMITER},
					{book+ID_PARTS_DELIMITER+sessionAlias+ID_PARTS_DELIMITER+"XXX"},
					{book+ID_PARTS_DELIMITER+sessionAlias+ID_PARTS_DELIMITER+"XXX"+ID_PARTS_DELIMITER},
					{book+ID_PARTS_DELIMITER+sessionAlias+ID_PARTS_DELIMITER+"XXX"+ID_PARTS_DELIMITER+"NNN"},
					{book+ID_PARTS_DELIMITER+sessionAlias+ID_PARTS_DELIMITER+"XXX"+ID_PARTS_DELIMITER+seq},
					{book+ID_PARTS_DELIMITER+sessionAlias+ID_PARTS_DELIMITER+direction.getLabel()},
					{book+ID_PARTS_DELIMITER+sessionAlias+ID_PARTS_DELIMITER+direction.getLabel()+ID_PARTS_DELIMITER},
					{book+ID_PARTS_DELIMITER+sessionAlias+ID_PARTS_DELIMITER+direction.getLabel()+ID_PARTS_DELIMITER+"NNN"}
				};
	}
	
	
	@Test
	public void idToString()
	{
		StoredMessageId id = new StoredMessageId(book, sessionAlias, direction, timestamp, messageSeq);
		Assert.assertEquals(id.toString(), stringId);
	}
	
	@Test
	public void idFromString() throws CradleIdException
	{
		StoredMessageId id = new StoredMessageId(book, sessionAlias, direction, timestamp, messageSeq),
				fromString = StoredMessageId.fromString(stringId);
		Assert.assertEquals(fromString, id);
	}
	
	@Test
	public void idFromStringWithColon() throws CradleIdException
	{
		StoredMessageId id = new StoredMessageId(book, sessionAliasWithColon, direction, timestamp, messageSeq),
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
	public void correctSessionAlias() throws CradleIdException
	{
		StoredMessageId id = StoredMessageId.fromString(stringIdWithColon);
		Assert.assertEquals(id.getSessionAlias(), sessionAliasWithColon);
	}
	
	@Test
	public void correctSequence() throws CradleIdException
	{
		StoredMessageId id = StoredMessageId.fromString(stringId);
		Assert.assertEquals(id.getSequence(), messageSeq);
	}
}

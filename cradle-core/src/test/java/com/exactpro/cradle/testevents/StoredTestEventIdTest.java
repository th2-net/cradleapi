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

import org.apache.commons.lang3.StringUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.exactpro.cradle.BookId;
import com.exactpro.cradle.utils.CradleIdException;

import static com.exactpro.cradle.messages.StoredMessageId.*;

import java.time.Instant;
import java.util.UUID;

public class StoredTestEventIdTest
{
	private BookId book;
	private String scope;
	private Instant timestamp;
	private String textId,
			fullId;
	
	@BeforeClass
	public void prepare()
	{
		book = new BookId("book1");
		scope = "scope1";
		timestamp = Instant.EPOCH;
		textId = UUID.randomUUID().toString();
		
		fullId = StringUtils.joinWith(ID_PARTS_DELIMITER, 
				book, 
				scope, 
				StoredTestEventIdUtils.timestampToString(timestamp), 
				textId);
	}
	
	@DataProvider(name = "escaped")
	public Object[][] escaped()
	{
		return new Object[][]
				{
					{new StoredTestEventId(new BookId("book1:main"), "FT:Normal", timestamp, "id:1")},
					{new StoredTestEventId(new BookId("book1\\final"), "FT\\Normal", timestamp, "id\\1")},
					{new StoredTestEventId(new BookId("book1:main\\client"), "FT:Normal\\BigButton", timestamp, "id\\1:2")}
				};
	}
	
	@DataProvider(name = "invalid IDs")
	public Object[][] invalidIds()
	{
		return new Object[][]
				{
					{""},
					{StringUtils.joinWith(ID_PARTS_DELIMITER, book, scope, "20211020100000123456789")},
					{StringUtils.joinWith(ID_PARTS_DELIMITER, book+"\\", scope, "20211020100000123456789", textId)},
					{StringUtils.joinWith(ID_PARTS_DELIMITER, "\\"+book, scope, "20211020100000123456789", textId)},
					{StringUtils.joinWith(ID_PARTS_DELIMITER, book, scope, "tmstmp", textId)},
					{StringUtils.joinWith(ID_PARTS_DELIMITER, book, scope, "20211020100000123456789", textId+"\\")},
				};
	}
	
	
	@Test
	public void idToString()
	{
		StoredTestEventId id = new StoredTestEventId(book, scope, timestamp, textId);
		Assert.assertEquals(id.toString(), fullId);
	}
	
	@Test
	public void idFromString() throws CradleIdException
	{
		StoredTestEventId id = new StoredTestEventId(book, scope, timestamp, textId),
				fromString = StoredTestEventId.fromString(fullId);
		Assert.assertEquals(fromString, id);
	}
	
	@Test(dataProvider = "escaped")
	public void idFromEscapedString(StoredTestEventId id) throws CradleIdException
	{
		String s = id.toString();
		StoredTestEventId fromString = StoredTestEventId.fromString(s);
		Assert.assertEquals(fromString, id);
	}
	
	@Test(dataProvider = "invalid IDs",	
			expectedExceptions = {CradleIdException.class})
	public void idFromStringChecks(String s) throws CradleIdException
	{
		StoredTestEventId.fromString(s);
	}
}

/*
 * Copyright 2022-2022 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.cradle.cassandra.dao.books;

import com.exactpro.cradle.BookToAdd;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.time.Instant;

import static org.testng.Assert.*;

public class BookEntityTest
{
	@DataProvider(name = "nameProvider")
	protected Object[][] nameProvider()
	{
		return new Object[][] {
				{"_", "\"book__\""},
				{"4", "\"book_4\""},
				{"name", "\"book_name\""},
				{"Name", "\"book_name\""},
				{"NaMe", "\"book_name\""},
				{"\"NaMe\"", "\"book_name\""},
				{"\"book_NaMe\"", "\"book_name\""},
		};
	}

	@Test(dataProvider = "nameProvider")
	public void keyspaceNameTest(String name, String keyspaceName)
	{
		BookEntity entity = new BookEntity(new BookToAdd(name, Instant.now(), "firstPage"));
		assertEquals(entity.getKeyspaceName(), keyspaceName);
	}

}
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

package com.exactpro.cradle.cassandra.dao.messages;

import com.exactpro.cradle.BookId;
import com.exactpro.cradle.Direction;
import com.exactpro.cradle.PageId;
import com.exactpro.cradle.messages.*;
import com.exactpro.cradle.utils.CradleStorageException;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.recursive.comparison.RecursiveComparisonConfiguration;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.time.Instant;
import java.util.Collection;
import java.util.zip.DataFormatException;

import static com.exactpro.cradle.cassandra.TestUtils.createContent;

public class MessageEntityUtilsTest
{
	private final BookId book = new BookId("Test_Book_1");
	private final PageId page = new PageId(book, "Test_Page_1");
	private final String sessionAlias = "TEST_Session";
	private final int contentChunk = 20,
			maxUncompressedSize = 2000,
			content40 = 40,
			content45 = 45;
	private long sequence = 0L;

	@DataProvider(name = "batches")
	public Object[][] batches() throws CradleStorageException
	{
		MessageToStoreBuilder builder = MessageToStore.builder()
				.bookId(page.getBookId())
				.sessionAlias(sessionAlias)
				.timestamp(Instant.now())
				.direction(Direction.FIRST);
		return new Object[][]
				{
					{MessageBatchToStore.singleton(builder.content(createContent(content40)).sequence(++sequence).build())}	
				};
	}
	
	@Test(dataProvider = "batches")
	public void testToEntities(MessageBatch batch) throws IOException, DataFormatException
	{
		Collection<MessageBatchEntity> entities = 
				MessageEntityUtils.toEntities(batch, page, maxUncompressedSize, contentChunk);
		StoredMessageBatch messageBatch = MessageEntityUtils.toStoredMessageBatch(entities, page);

		RecursiveComparisonConfiguration config = new RecursiveComparisonConfiguration();

		Assertions.assertThat(batch).usingRecursiveComparison(config).isEqualTo(messageBatch);
	}
}
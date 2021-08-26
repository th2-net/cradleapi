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

import java.time.Instant;

import org.assertj.core.api.Assertions;
import org.testng.annotations.Test;

import com.exactpro.cradle.BookId;
import com.exactpro.cradle.Direction;
import com.exactpro.cradle.messages.StoredMessageId;
import com.exactpro.cradle.utils.CradleStorageException;

public class TestEventBuilderTest
{
	private final BookId bookId = new BookId("Book1");
	
	@Test
	public void singleBuilderIsReset() throws CradleStorageException
	{
		TestEventSingleToStoreBuilder builder = new TestEventSingleToStoreBuilder();
		builder.id(new StoredTestEventId(bookId, "Scope1", Instant.now(), "123"))
				.name("Event1")
				.parentId(new StoredTestEventId(bookId, "Scope2", Instant.EPOCH, "234"))
				.type("Type1")
				.success(true)
				.endTimestamp(Instant.now())
				.message(new StoredMessageId(bookId, "session1", Direction.FIRST, Instant.now(), 1))
				.content("Dummy event".getBytes())
				.build();
		
		Assertions.assertThat(builder)
				.usingRecursiveComparison()
				.isEqualTo(new TestEventSingleToStoreBuilder());
	}
	
	@Test
	public void batchBuilderIsReset() throws CradleStorageException
	{
		TestEventBatchToStoreBuilder builder = new TestEventBatchToStoreBuilder();
		builder.id(new StoredTestEventId(bookId, "Scope1", Instant.now(), "123"))
				.name("Event1")
				.parentId(new StoredTestEventId(bookId, "Scope2", Instant.EPOCH, "234"))
				.type("Type1")
				.build();
		
		Assertions.assertThat(builder)
				.usingRecursiveComparison()
				.isEqualTo(new TestEventBatchToStoreBuilder());
	}
}
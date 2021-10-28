/*
 * Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.cradle;

import java.time.Instant;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.exactpro.cradle.messages.MessageBatchToStore;
import com.exactpro.cradle.testevents.StoredTestEventId;
import com.exactpro.cradle.testevents.TestEventBatchToStore;
import com.exactpro.cradle.utils.CradleStorageException;

public class CradleEntitiesFactoryTest
{
	private final int maxMessageBatchSize = 123,
			maxEventBatchSize = 234;
	private CradleEntitiesFactory factory;
	
	@BeforeClass
	public void prepare()
	{
		factory = new CradleEntitiesFactory(maxMessageBatchSize, maxEventBatchSize);
	}
	
	@Test
	public void createMessageBatch()
	{
		MessageBatchToStore batch = factory.messageBatch();
		Assert.assertEquals(batch.getSpaceLeft(), maxMessageBatchSize, 
				"CradleEntitiesFactory creates MessageBatchToStore with maximum size defined in factory constructor");
	}
	
	@Test
	public void createTestEventBatch() throws CradleStorageException
	{
		BookId bookId = new BookId("Book1");
		String scope = "Scope1";
		Instant timestamp = Instant.EPOCH;
		TestEventBatchToStore batch = factory.testEventBatchBuilder()
				.id(bookId, scope, timestamp, "test_event1")
				.name("test_event")
				.parentId(new StoredTestEventId(bookId, scope, timestamp.plusNanos(1), "parent_event1"))
				.build();
		Assert.assertEquals(batch.getSpaceLeft(), maxEventBatchSize, 
				"CradleEntitiesFactory creates TestEventBatchToStore with maximum size defined in factory constructor");
	}
}

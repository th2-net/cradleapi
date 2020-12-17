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

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.exactpro.cradle.messages.StoredMessageBatch;
import com.exactpro.cradle.testevents.StoredTestEventBatch;
import com.exactpro.cradle.testevents.StoredTestEventId;
import com.exactpro.cradle.testevents.TestEventBatchToStore;
import com.exactpro.cradle.utils.CradleStorageException;

public class CradleObjectsFactoryTest
{
	private final long maxMessageBatchSize = 123,
			maxEventBatchSize = 234;
	private CradleObjectsFactory factory;
	
	@BeforeClass
	public void prepare()
	{
		factory = new CradleObjectsFactory(maxMessageBatchSize, maxEventBatchSize);
	}
	
	@Test
	public void createMessageBatch()
	{
		StoredMessageBatch batch = factory.createMessageBatch();
		Assert.assertEquals(batch.getSpaceLeft(), maxMessageBatchSize, 
				"CradleObjectsFactory creates StoredMessageBatch with maximum size defined in factory constructor");
	}
	
	@Test
	public void createTestEventBatch() throws CradleStorageException
	{
		StoredTestEventBatch batch = factory.createTestEventBatch(TestEventBatchToStore.builder().id(new StoredTestEventId("test_event1"))
				.name("test_event")
				.parentId(new StoredTestEventId("parent_event1"))
				.build());
		Assert.assertEquals(batch.getSpaceLeft(), maxEventBatchSize, 
				"CradleObjectsFactory creates StoredTestEventBatch with maximum size defined in factory constructor");
	}
}

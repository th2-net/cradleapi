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

package com.exactpro.cradle.cassandra;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.exactpro.cradle.CradleStorage;
import com.exactpro.cradle.cassandra.connection.CassandraConnection;
import com.exactpro.cradle.messages.StoredMessageBatch;
import com.exactpro.cradle.testevents.StoredTestEventBatch;
import com.exactpro.cradle.testevents.StoredTestEventId;
import com.exactpro.cradle.testevents.TestEventBatchToStore;
import com.exactpro.cradle.utils.CradleStorageException;

public class CassandraObjectsFactoryTest
{
	private final long maxMessageBatchSize = 999,
			maxEventBatchSize = 888;
	private CradleStorage storage;
	
	@BeforeClass
	public void prepare()
	{
		CassandraStorageSettings settings = new CassandraStorageSettings();
		settings.setMaxMessageBatchSize(maxMessageBatchSize);
		settings.setMaxTestEventBatchSize(maxEventBatchSize);
		storage = new CassandraCradleStorage(new CassandraConnection(), settings);
	}
	
	@Test
	public void createMessageBatch()
	{
		StoredMessageBatch batch = storage.getObjectsFactory().createMessageBatch();
		Assert.assertEquals(batch.getSpaceLeft(), maxMessageBatchSize, 
				"CradleObjectsFactory of CassandraCradleStorage creates StoredMessageBatch with maximum size defined in storage settings");
	}
	
	@Test
	public void createTestEventBatch() throws CradleStorageException
	{
		StoredTestEventBatch batch = storage.getObjectsFactory().createTestEventBatch(TestEventBatchToStore.builder()
				.id(new StoredTestEventId("test_event1"))
				.name("test_event")
				.parentId(new StoredTestEventId("parent_event1"))
				.build());
		Assert.assertEquals(batch.getSpaceLeft(), maxEventBatchSize, 
				"CradleObjectsFactory of CassandraCradleStorage creates StoredTestEventBatch with maximum size defined in storage settings");
	}
}

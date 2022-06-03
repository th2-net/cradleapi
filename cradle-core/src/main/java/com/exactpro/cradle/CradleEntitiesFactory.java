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

package com.exactpro.cradle;

import com.exactpro.cradle.messages.MessageBatchToStore;
import com.exactpro.cradle.messages.MessageToStore;
import com.exactpro.cradle.testevents.StoredTestEventId;
import com.exactpro.cradle.testevents.TestEventBatchToStore;
import com.exactpro.cradle.testevents.TestEventBatchToStoreBuilder;
import com.exactpro.cradle.utils.CradleStorageException;

/**
 * Factory to create entities to be used with {@link CradleStorage}. Created objects will conform with particular CradleStorage settings.
 */
public class CradleEntitiesFactory
{
	private final int maxMessageBatchSize,
			maxTestEventBatchSize;
	
	/**
	 * Creates new factory for entities to be used with {@link CradleStorage}
	 * @param maxMessageBatchSize maximum size of messages (in bytes) that {@link MessageBatchToStore} can hold
	 * @param maxTestEventBatchSize maximum size of test events (in bytes) that {@link TestEventBatchToStore} can hold
	 */
	public CradleEntitiesFactory(int maxMessageBatchSize, int maxTestEventBatchSize)
	{
		this.maxMessageBatchSize = maxMessageBatchSize;
		this.maxTestEventBatchSize = maxTestEventBatchSize;
	}
	
	
	public MessageBatchToStore messageBatch()
	{
		return new MessageBatchToStore(maxMessageBatchSize);
	}
	
	public MessageBatchToStore singletonMessageBatch(MessageToStore message) throws CradleStorageException
	{
		return MessageBatchToStore.singleton(message, maxMessageBatchSize);
	}
	
	public TestEventBatchToStore testEventBatch(StoredTestEventId id, String name, StoredTestEventId parentId) throws CradleStorageException
	{
		return new TestEventBatchToStore(id, name, parentId, maxTestEventBatchSize);
	}
	
	public TestEventBatchToStoreBuilder testEventBatchBuilder()
	{
		return new TestEventBatchToStoreBuilder(maxTestEventBatchSize);
	}
}

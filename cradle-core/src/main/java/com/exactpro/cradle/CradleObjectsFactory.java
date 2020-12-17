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

import com.exactpro.cradle.messages.StoredMessageBatch;
import com.exactpro.cradle.testevents.StoredTestEventBatch;
import com.exactpro.cradle.testevents.StoredTestEventSingle;
import com.exactpro.cradle.testevents.StoredTestEventWithContent;
import com.exactpro.cradle.testevents.TestEventBatchToStore;
import com.exactpro.cradle.testevents.TestEventBatchToStoreBuilder;
import com.exactpro.cradle.testevents.TestEventToStore;
import com.exactpro.cradle.testevents.TestEventToStoreBuilder;
import com.exactpro.cradle.utils.CradleStorageException;

/**
 * Factory to create objects to be used with {@link CradleStorage}. Created objects will conform with particular CradleStorage settings.
 */
public class CradleObjectsFactory
{
	private final long maxMessageBatchSize,
			maxTestEventBatchSize;
	
	/**
	 * Creates new factory for objects to be used with {@link CradleStorage}
	 * @param maxMessageBatchSize maximum size of messages (in bytes) that {@link StoredMessageBatch} can hold
	 * @param maxTestEventBatchSize maximum size of test events (in bytes) that {@link StoredTestEventBatch} can hold
	 */
	public CradleObjectsFactory(long maxMessageBatchSize, long maxTestEventBatchSize)
	{
		this.maxMessageBatchSize = maxMessageBatchSize;
		this.maxTestEventBatchSize = maxTestEventBatchSize;
	}
	
	
	public StoredMessageBatch createMessageBatch()
	{
		return new StoredMessageBatch(maxMessageBatchSize);
	}
	
	public StoredTestEventSingle createTestEvent(StoredTestEventWithContent eventData) throws CradleStorageException
	{
		return new StoredTestEventSingle(eventData);
	}
	
	public StoredTestEventBatch createTestEventBatch(TestEventBatchToStore batchData) throws CradleStorageException
	{
		return new StoredTestEventBatch(batchData, maxTestEventBatchSize);
	}
}

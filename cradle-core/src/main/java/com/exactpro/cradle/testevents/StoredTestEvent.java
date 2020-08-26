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

package com.exactpro.cradle.testevents;

import java.time.Instant;

import com.exactpro.cradle.utils.CradleStorageException;

/**
 * Interface for all stored test events. Provides access only to event meta-data. Event content is specific to event
 */
public interface StoredTestEvent extends MinimalTestEventFields
{
	Instant getStartTimestamp();
	Instant getEndTimestamp();
	boolean isSuccess();
	
	
	public static StoredTestEventSingle newStoredTestEventSingle(TestEventToStore event) throws CradleStorageException
	{
		return new StoredTestEventSingle(event);
	}
	
	public static StoredTestEventBatch newStoredTestEventBatch(TestEventBatchToStore batchData) throws CradleStorageException
	{
		return new StoredTestEventBatch(batchData);
	}
}

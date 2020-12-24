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
import com.exactpro.cradle.utils.CradleStorageException;

/**
 * Class to manage access to Cradle storage and its features.
 */
public abstract class CradleManager
{
	private CradleStorage storage;
	
	public CradleManager()
	{
	}
	
	/**
	 * Creates {@link CradleStorage} object to work with Cradle
	 * @param maxMessageBatchSize maximum size of {@link StoredMessageBatch} (in bytes) allowed for {@link CradleStorage} while writing data
	 * @param maxTestEventBatchSize maximum size of {@link StoredTestEventBatch} (in bytes) allowed for {@link CradleStorage} while writing data
	 * @return instance of CradleStorage to read/write data
	 */
	protected abstract CradleStorage createStorage(long maxMessageBatchSize, long maxTestEventBatchSize);
	
	
	/**
	 * Initializes manager to get access to Cradle storage using given name of data instance
	 * @param instanceName name of data instance
	 * @param prepareStorage flag that indicates if underlying storage on disk can be created or its structure can be updated, if needed
	 * @param maxMessageBatchSize maximum size of {@link StoredMessageBatch} (in bytes) allowed for {@link CradleStorage} while writing data
	 * @param maxTestEventBatchSize maximum size of {@link StoredTestEventBatch} (in bytes) allowed for {@link CradleStorage} while writing data
	 * @throws CradleStorageException if access to Cradle storage cannot be established
	 */
	public void init(String instanceName, boolean prepareStorage, long maxMessageBatchSize, long maxTestEventBatchSize) throws CradleStorageException
	{
		initStart(instanceName, prepareStorage, maxMessageBatchSize, maxTestEventBatchSize);
		initFinish();
	}
	
	/**
	 * Initializes manager to get access to Cradle storage using given name of data instance
	 * @param instanceName name of data instance
	 * @param prepareStorage flag that indicates if underlying storage on disk can be created or its structure can be updated, if needed
	 * @throws CradleStorageException if access to Cradle storage cannot be established
	 */
	public void init(String instanceName, boolean prepareStorage) throws CradleStorageException
	{
		initStart(instanceName, prepareStorage, StoredMessageBatch.DEFAULT_MAX_BATCH_SIZE, StoredTestEventBatch.DEFAULT_MAX_BATCH_SIZE);
		initFinish();
	}
	
	/**
	 * Initializes manager to get access to Cradle storage using given name of data instance.
	 * Storage on disk won't be created, if absent, and its structure won't be updated, even if needed
	 * @param instanceName name of data instance
	 * @throws CradleStorageException if access to Cradle storage cannot be established
	 */
	public void init(String instanceName) throws CradleStorageException
	{
		initStart(instanceName, false, StoredMessageBatch.DEFAULT_MAX_BATCH_SIZE, StoredTestEventBatch.DEFAULT_MAX_BATCH_SIZE);
		initFinish();
	}
	
	/**
	 * Closes access to Cradle storage, closing all related connections, flushing buffers, etc.
	 * @throws CradleStorageException if there was error during Cradle storage disposal, which may mean issue with data flushing, unexpected connection break, etc.
	 */
	public void dispose() throws CradleStorageException
	{
		if (storage != null)
			storage.dispose();
	}
	
	
	/**
	 * Starts initialization to get access to Cradle storage using given name of data instance
	 * @param instanceName name of data instance
	 * @param prepareStorage flag that indicates if underlying storage on disk can be created or its structure can be updated, if needed
	 * @param maxMessageBatchSize maximum size of {@link StoredMessageBatch} (in bytes) allowed for {@link CradleStorage} while writing data
	 * @param maxTestEventBatchSize maximum size of {@link StoredTestEventBatch} (in bytes) allowed for {@link CradleStorage} while writing data
	 * @throws CradleStorageException if access to Cradle storage cannot be established
	 */
	public void initStart(String instanceName, boolean prepareStorage, long maxMessageBatchSize, long maxTestEventBatchSize) throws CradleStorageException
	{
		storage = createStorage(maxMessageBatchSize, maxTestEventBatchSize);
		if (storage != null)
			storage.init(instanceName, prepareStorage);
	}
	
	/**
	 * Starts initialization to get access to Cradle storage using given name of data instance
	 * @param instanceName name of data instance
	 * @param prepareStorage flag that indicates if underlying storage on disk can be created or its structure can be updated, if needed
	 * @throws CradleStorageException if access to Cradle storage cannot be established
	 */
	public void initStart(String instanceName, boolean prepareStorage) throws CradleStorageException
	{
		initStart(instanceName, prepareStorage, StoredMessageBatch.DEFAULT_MAX_BATCH_SIZE, StoredTestEventBatch.DEFAULT_MAX_BATCH_SIZE);
	}
	
	/**
	 * Starts initialization to get access to Cradle storage using given name of data instance.
	 * Storage on disk won't be created, if absent, and its structure won't be updated, even if needed
	 * @param instanceName name of data instance
	 * @throws CradleStorageException if access to Cradle storage cannot be established
	 */
	public void initStart(String instanceName) throws CradleStorageException
	{
		initStart(instanceName, false, StoredMessageBatch.DEFAULT_MAX_BATCH_SIZE, StoredTestEventBatch.DEFAULT_MAX_BATCH_SIZE);
	}
	
	/**
	 * Finishes initialization by switching storage access to working state. After that storage can be used to read/write data
	 */
	public void initFinish()
	{
		if (storage != null)
			storage.initFinish();
	}
	
	
	/**
	 * @return {@link CradleStorage} object to read/write data
	 */
	public CradleStorage getStorage()
	{
		return storage;
	}
}

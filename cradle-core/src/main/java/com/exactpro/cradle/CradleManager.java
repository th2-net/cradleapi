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
	 * @return instance of CradleStorage to read/write data
	 */
	protected abstract CradleStorage createStorage();
	
	/**
	 * Initializes manager to get access to Cradle storage using given name of application instance
	 * @param instanceName name of application instance
	 * @throws CradleStorageException if access to Cradle storage cannot be established
	 */
	public void init(String instanceName) throws CradleStorageException
	{
		initStart(instanceName);
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
	 * Starts initialization to get access to Cradle storage using given name of application instance
	 * @param instanceName name of application instance
	 * @throws CradleStorageException if access to Cradle storage cannot be established
	 */
	public void initStart(String instanceName) throws CradleStorageException
	{
		storage = createStorage();
		if (storage != null)
			storage.init(instanceName);
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

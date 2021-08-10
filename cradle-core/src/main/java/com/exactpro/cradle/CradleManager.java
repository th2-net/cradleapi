/*
 * Copyright 2020-2021 Exactpro (Exactpro Systems Limited)
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
public abstract class CradleManager implements AutoCloseable
{
	private CradleStorage storage;
	
	/**
	 * Creates {@link CradleStorage} object to work with Cradle
	 * @return instance of CradleStorage to read/write data
	 */
	protected abstract CradleStorage createStorage() throws CradleStorageException;
	
	
	/**
	 * Initializes manager to get access to Cradle storage.
	 * @throws CradleStorageException if access to Cradle storage cannot be established
	 */
	public void init() throws CradleStorageException
	{
		if (storage == null)
		{
			storage = createStorage();
			storage.init();
		}
	}
	
	/**
	 * Closes access to Cradle storage, disposing all related connections, flushing buffers, etc.
	 * @throws Exception if there was error during Cradle storage disposal, which may mean issue with data flushing, unexpected connection break, etc.
	 */
	@Override
	public void close() throws Exception
	{
		if (storage != null)
		{
			storage.dispose();
			storage = null;
		}
	}
	
	public CradleStorage getStorage() throws CradleStorageException
	{
		return storage;
	}
}

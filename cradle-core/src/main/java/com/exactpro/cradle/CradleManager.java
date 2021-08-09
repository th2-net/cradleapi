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
	private final CradleStorage storage;
	
	public CradleManager() throws CradleStorageException
	{
		storage = createStorage();
	}
	
	/**
	 * Creates {@link CradleStorage} object to work with Cradle
	 * @return instance of CradleStorage to read/write data
	 */
	protected abstract CradleStorage createStorage() throws CradleStorageException;
	
	
	public CradleStorage getStorage() throws CradleStorageException
	{
		return storage;
	}
}

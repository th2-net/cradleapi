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

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.cradle.messages.StoredMessageBatch;
import com.exactpro.cradle.testevents.StoredTestEventBatch;
import com.exactpro.cradle.utils.CradleStorageException;

/**
 * Class to manage access to Cradle storage and its features.
 */
public abstract class CradleManager implements AutoCloseable
{
	private static final Logger logger = LoggerFactory.getLogger(CradleManager.class);
	
	private final Map<String, CradleStorage> storages = new ConcurrentHashMap<>();
	private volatile boolean closed = false;
	
	public CradleManager()
	{
	}
	
	/**
	 * Creates {@link CradleStorage} object to work with given Cradle book
	 * @return instance of CradleStorage to read/write data
	 */
	protected abstract CradleStorage createStorage(String book);
	
	
	/**
	 * Returns {@link CradleStorage} to work with given Cradle book. Creates connections and facilities to access the book, if needed
	 * @param book to work with
	 * @return instance of {@link CradleStorage} bound to given book to read/write data
	 * @throws CradleStorageException if access to Cradle storage cannot be established
	 */
	public CradleStorage getStorage(String book) throws CradleStorageException
	{
		checkClosed();
		
		CradleStorage result = storages.get(book);
		if (result == null || result.isDisposed())
		{
			synchronized (storages)
			{
				checkClosed();
				
				result = storages.get(book);
				if (result == null || result.isDisposed())
				{
					logger.info("Storage for book '{}' is absent, creating it", book);
					result = createStorage(book);
					storages.put(book, result);
				}
			}
		}
		return result;
	}
	
	/**
	 * Disposes access to Cradle, closing all related connections, flushing buffers, etc.
	 * @throws IOException if there was error during Cradle storage disposal, which may mean issue with data flushing, unexpected connection break, etc.
	 */
	@Override
	public void close() throws CradleStorageException
	{
		logger.info("Closing manager");
		closed = true;
		synchronized (storages)
		{
			for (CradleStorage st : storages.values())
			{
				if (!st.isDisposed())
					st.dispose();
			}
			
			storages.clear();
		}
		logger.info("Manager closed");
	}
	
	
	private void checkClosed() throws CradleStorageException
	{
		if (closed)
			throw new CradleStorageException("Manager is closed");
	}
}

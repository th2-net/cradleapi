/******************************************************************************
 * Copyright (c) 2009-2020, Exactpro Systems LLC
 * www.exactpro.com
 * Build Software to Test Software
 *
 * All rights reserved.
 * This is unpublished, licensed software, confidential and proprietary 
 * information which is the property of Exactpro Systems LLC or its licensors.
 ******************************************************************************/

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

/******************************************************************************
 * Copyright (c) 2009-2020, Exactpro Systems LLC
 * www.exactpro.com
 * Build Software to Test Software
 *
 * All rights reserved.
 * This is unpublished, licensed software, confidential and proprietary 
 * information which is the property of Exactpro Systems LLC or its licensors.
 ******************************************************************************/

package com.exactpro.cradle.cassandra;

import com.exactpro.cradle.CradleManager;
import com.exactpro.cradle.CradleStorage;
import com.exactpro.cradle.cassandra.connection.CassandraConnection;
import com.exactpro.cradle.cassandra.connection.CassandraConnectionSettings;

public class CassandraCradleManager extends CradleManager
{
	private final CassandraConnection connection;

	public CassandraCradleManager(CassandraConnection connection)
	{
		super();
		this.connection = connection;
	}

	@Override
	protected CradleStorage createStorage()
	{
		CassandraConnectionSettings settings = connection.getSettings();
		CassandraStorageSettings storageSettings = new CassandraStorageSettings(settings.getKeyspace(),
				settings.getNetworkTopologyStrategy(),
				settings.getTimeout() <= 0 ? CassandraStorageSettings.DEFAULT_TIMEOUT : settings.getTimeout(),
				settings.getWriteConsistencyLevel() == null ? CassandraStorageSettings.DEFAULT_CONSISTENCY_LEVEL : settings.getWriteConsistencyLevel(),
				settings.getReadConsistencyLevel() == null ? CassandraStorageSettings.DEFAULT_CONSISTENCY_LEVEL : settings.getReadConsistencyLevel());
		
		return new CassandraCradleStorage(connection, storageSettings);
	}
}

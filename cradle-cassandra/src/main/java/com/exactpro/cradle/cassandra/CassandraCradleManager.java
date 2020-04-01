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
import com.exactpro.cradle.MessageNavigator;
import com.exactpro.cradle.cassandra.connection.CassandraConnection;

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
		return new CassandraCradleStorage(connection, new CassandraStorageSettings(connection.getSettings().getKeyspace()));
	}
}

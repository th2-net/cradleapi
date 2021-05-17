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

package com.exactpro.cradle.cassandra;

import com.exactpro.cradle.CradleManager;
import com.exactpro.cradle.CradleStorage;
import com.exactpro.cradle.cassandra.connection.CassandraConnectionSettings;
import com.exactpro.cradle.cassandra.connection.CassandraConnectionSettings;
import com.exactpro.cradle.cassandra.CassandraStorageSettings;

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
	protected CradleStorage createStorage(long maxMessageBatchSize, long maxTestEventBatchSize)
	{
		CassandraConnectionSettings settings = connection.getSettings();
		CassandraStorageSettings storageSettings = createStorageSettings(settings, maxMessageBatchSize, maxTestEventBatchSize);
		
		return new CassandraCradleStorage(connection, storageSettings);
	}
	
	protected CassandraStorageSettings createStorageSettings(CassandraConnectionSettings connectionSettings, long maxMessageBatchSize, long maxTestEventBatchSize)
	{
		CassandraStorageSettings result = new CassandraStorageSettings(connectionSettings.getKeyspace(),
				connectionSettings.getNetworkTopologyStrategy(),
				connectionSettings.getTimeout() <= 0 ? CassandraStorageSettings.DEFAULT_TIMEOUT : connectionSettings.getTimeout(),
				connectionSettings.getWriteConsistencyLevel() == null ? CassandraStorageSettings.DEFAULT_CONSISTENCY_LEVEL : connectionSettings.getWriteConsistencyLevel(),
				connectionSettings.getReadConsistencyLevel() == null ? CassandraStorageSettings.DEFAULT_CONSISTENCY_LEVEL : connectionSettings.getReadConsistencyLevel());
		if (maxMessageBatchSize > 0)
			result.setMaxMessageBatchSize(maxMessageBatchSize);
		if (maxTestEventBatchSize > 0)
			result.setMaxTestEventBatchSize(maxTestEventBatchSize);
		return result;
	}
}

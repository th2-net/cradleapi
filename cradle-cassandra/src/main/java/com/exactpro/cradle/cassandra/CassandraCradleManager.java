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

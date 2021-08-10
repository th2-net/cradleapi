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

package com.exactpro.cradle.cassandra;

import com.exactpro.cradle.CradleManager;
import com.exactpro.cradle.cassandra.connection.CassandraConnectionSettings;
import com.exactpro.cradle.utils.CradleStorageException;

public class CassandraCradleManager extends CradleManager
{
	private final CassandraConnectionSettings connectionSettings;
	private final CassandraStorageSettings storageSettings;
	
	public CassandraCradleManager(CassandraConnectionSettings connectionSettings, CassandraStorageSettings storageSettings, boolean prepareStorage)
	{
		this.connectionSettings = new CassandraConnectionSettings(connectionSettings);
		this.storageSettings = new CassandraStorageSettings(storageSettings);
	}

	@Override
	protected CassandraCradleStorage createStorage() throws CradleStorageException
	{
		return new CassandraCradleStorage(connectionSettings, storageSettings);
	}
}

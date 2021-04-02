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

package com.exactpro.cradle.cassandra.amazon;

import com.exactpro.cradle.CradleStorage;
import com.exactpro.cradle.cassandra.CassandraCradleManager;
import com.exactpro.cradle.cassandra.CassandraStorageSettings;
import com.exactpro.cradle.cassandra.connection.CassandraConnection;
import com.exactpro.cradle.cassandra.connection.CassandraConnectionSettings;

public class AmazonCradleManager extends CassandraCradleManager
{
	public AmazonCradleManager(CassandraConnection connection)
	{
		super(connection);
	}

	@Override
	protected CradleStorage createStorage(long maxMessageBatchSize, long maxTestEventBatchSize)
	{
		CassandraConnectionSettings settings = connection.getSettings();
		CassandraStorageSettings
				storageSettings = createStorageSettings(settings, maxMessageBatchSize, maxTestEventBatchSize);

		return new AmazonCradleStorage(connection, storageSettings);
	}
}

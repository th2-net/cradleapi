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

package com.exactpro.cradle.cassandra.iterators;

import com.datastax.oss.driver.api.core.MappedAsyncPagingIterable;
import com.exactpro.cradle.daomodule.dao.testevents.TestEventEntity;
import com.exactpro.cradle.testevents.StoredTestEventWrapper;
import com.exactpro.cradle.utils.CradleStorageException;

import java.io.IOException;

public class TestEventsDataIterator extends ConvertingPagedIterator<StoredTestEventWrapper, TestEventEntity>
{
	public TestEventsDataIterator(MappedAsyncPagingIterable<TestEventEntity> rows)
	{
		super(rows);
	}

	@Override
	protected StoredTestEventWrapper convertEntity(TestEventEntity entity) throws IOException
	{
		try
		{
			return entity.toStoredTestEventWrapper();
		}
		catch (CradleStorageException e)
		{
			throw new IOException("Could not get test event", e);
		}
	}
}

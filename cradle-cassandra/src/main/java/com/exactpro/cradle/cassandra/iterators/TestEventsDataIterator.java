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
import com.exactpro.cradle.CradleObjectsFactory;
import com.exactpro.cradle.cassandra.dao.testevents.TestEventEntity;
import com.exactpro.cradle.cassandra.dao.testevents.converters.TestEventConverter;
import com.exactpro.cradle.cassandra.retries.PagingSupplies;
import com.exactpro.cradle.testevents.StoredTestEventWrapper;
import com.exactpro.cradle.utils.CradleStorageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;

public class TestEventsDataIterator extends ConvertingPagedIterator<StoredTestEventWrapper, TestEventEntity>
{
	private Logger logger = LoggerFactory.getLogger(TestEventsDataIterator.class);

	private final CradleObjectsFactory objectsFactory;
	private final Instant actualFrom;

	public TestEventsDataIterator(MappedAsyncPagingIterable<TestEventEntity> rows,
			PagingSupplies pagingSupplies, TestEventConverter converter,
			CradleObjectsFactory objectsFactory, Instant actualFrom, String queryInfo)
	{
		super(rows, pagingSupplies, converter, queryInfo);
		this.objectsFactory = objectsFactory;
		this.actualFrom = actualFrom;
	}

	@Override
	public StoredTestEventWrapper next() {
		StoredTestEventWrapper rtn = super.next();
		while (hasNext() && rtn.getStartTimestamp().isBefore(actualFrom)) {
			logger.debug("Skipping event with start timestamp {}, actual request was from {}", rtn.getStartTimestamp(), actualFrom);
			rtn = super.next();
		}

		return rtn;
	}

	@Override
	protected StoredTestEventWrapper convertEntity(TestEventEntity entity) throws IOException
	{
		try
		{
			return entity.toStoredTestEventWrapper(objectsFactory);
		}
		catch (CradleStorageException e)
		{
			throw new IOException("Could not get test event", e);
		}
	}
}

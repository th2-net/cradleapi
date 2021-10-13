/*
 * Copyright 2021-2021 Exactpro (Exactpro Systems Limited)
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
import com.exactpro.cradle.cassandra.dao.EntityConverter;
import com.exactpro.cradle.cassandra.dao.testevents.TestEventMetadataEntity;
import com.exactpro.cradle.cassandra.retries.PagingSupplies;
import com.exactpro.cradle.testevents.StoredTestEventMetadata;

import java.io.IOException;

public class TestEventsMetadataIterator extends ConvertingPagedIterator<StoredTestEventMetadata, TestEventMetadataEntity>
{
	public TestEventsMetadataIterator(
			MappedAsyncPagingIterable<TestEventMetadataEntity> rows,
			PagingSupplies pagingSupplies,
			EntityConverter<TestEventMetadataEntity> converter,
			String queryInfo)
	{
		super(rows, pagingSupplies, converter, queryInfo);
	}

	@Override
	protected StoredTestEventMetadata convertEntity(TestEventMetadataEntity entity) throws IOException
	{
		return entity.toStoredTestEventMetadata();
	}
}
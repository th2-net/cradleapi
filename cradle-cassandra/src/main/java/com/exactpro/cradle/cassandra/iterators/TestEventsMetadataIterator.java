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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.NoSuchElementException;

public class TestEventsMetadataIterator extends ConvertingPagedIterator<StoredTestEventMetadata, TestEventMetadataEntity>
{
	private final Logger logger = LoggerFactory.getLogger(TestEventsMetadataIterator.class);
	private final Instant actualFrom;
	private StoredTestEventMetadata preFetchedElement;

	public TestEventsMetadataIterator(
			MappedAsyncPagingIterable<TestEventMetadataEntity> rows,
			PagingSupplies pagingSupplies,
			EntityConverter<TestEventMetadataEntity> converter,
			Instant actualFrom,
			String queryInfo)
	{
		super(rows, pagingSupplies, converter, queryInfo);
		this.actualFrom = actualFrom;
	}

	private boolean skipToValid() {
		if (!super.hasNext()) {
			return false;
		}

		StoredTestEventMetadata nextEl = super.next();

		while (super.hasNext() && nextEl.getMaxStartTimestamp().isBefore(actualFrom)) {
			logger.trace("Skipping event with start timestamp {}, actual request was from {}", nextEl.getStartTimestamp(), actualFrom);
			nextEl = super.next();
		}

		if (nextEl.getMaxStartTimestamp().isBefore(actualFrom)) {
			return false;
		}

		preFetchedElement = nextEl;

		return true;
	}

	@Override
	public boolean hasNext() {
		if (preFetchedElement != null) {
			return true;
		}

		return skipToValid();
	}

	@Override
	public StoredTestEventMetadata next() {
		if (hasNext()) {
			StoredTestEventMetadata rtn = preFetchedElement;
			preFetchedElement = null;

			return rtn;
		}

		throw new NoSuchElementException("There are no more elements in iterator");
	}


	@Override
	protected StoredTestEventMetadata convertEntity(TestEventMetadataEntity entity) throws IOException
	{
		return entity.toStoredTestEventMetadata();
	}
}

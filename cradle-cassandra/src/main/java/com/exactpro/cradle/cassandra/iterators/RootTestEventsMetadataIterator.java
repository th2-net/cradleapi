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

package com.exactpro.cradle.cassandra.iterators;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.oss.driver.api.core.MappedAsyncPagingIterable;
import com.exactpro.cradle.cassandra.dao.testevents.RootTestEventEntity;
import com.exactpro.cradle.testevents.StoredTestEventMetadata;

public class RootTestEventsMetadataIterator implements Iterator<StoredTestEventMetadata>
{
	private final Logger logger = LoggerFactory.getLogger(RootTestEventsMetadataIterator.class);
	
	private MappedAsyncPagingIterable<RootTestEventEntity> rows;
	private Iterator<RootTestEventEntity> rowsIterator;
	
	public RootTestEventsMetadataIterator(MappedAsyncPagingIterable<RootTestEventEntity> rows)
	{
		this.rows = rows;
		this.rowsIterator = rows.currentPage().iterator();
	}
	
	@Override
	public boolean hasNext()
	{
		if (!rowsIterator.hasNext())
		{
			try
			{
				rowsIterator = fetchNextIterator();
			}
			catch (Exception e)
			{
				throw new RuntimeException("Error while getting next page of result", e);
			}
			
			if (rowsIterator == null || !rowsIterator.hasNext())
				return false;
		}
		return true;
	}
	
	@Override
	public StoredTestEventMetadata next()
	{
		logger.trace("Getting next root test event metadata");
		
		RootTestEventEntity r = rowsIterator.next();
		try
		{
			return r.toStoredTestEventMetadata();
		}
		catch (IOException e)
		{
			throw new RuntimeException("Error while getting next root test event metadata", e);
		}
	}
	
	
	private Iterator<RootTestEventEntity> fetchNextIterator() throws IllegalStateException, InterruptedException, ExecutionException
	{
		if (rows.hasMorePages())
		{
			rows = rows.fetchNextPage().toCompletableFuture().get();  //TODO: better to fetch next page in advance, not when current page ended
			return rows.currentPage().iterator();
		}
		return null;
	}
}

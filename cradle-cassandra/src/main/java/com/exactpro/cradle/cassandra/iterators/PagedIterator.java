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

import java.util.Iterator;
import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.oss.driver.api.core.MappedAsyncPagingIterable;

/**
 * Wrapper for asynchronous paging iterable to get entities retrieved from Cassandra
 * @param <E> - class of entities obtained from Cassandra
 */
public class PagedIterator<E> implements Iterator<E>
{
	private final Logger logger = LoggerFactory.getLogger(PagedIterator.class);
	
	private MappedAsyncPagingIterable<E> rows;
	private Iterator<E> rowsIterator;
	
	public PagedIterator(MappedAsyncPagingIterable<E> rows)
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
	public E next()
	{
		logger.trace("Getting next data row");
		return rowsIterator.next();
	}
	
	
	private Iterator<E> fetchNextIterator() throws IllegalStateException, InterruptedException, ExecutionException
	{
		if (rows.hasMorePages())
		{
			rows = rows.fetchNextPage().toCompletableFuture().get();  //TODO: better to fetch next page in advance, not when current page ended
			return rows.currentPage().iterator();
		}
		return null;
	}
}

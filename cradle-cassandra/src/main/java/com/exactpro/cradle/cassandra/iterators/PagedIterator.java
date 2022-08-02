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

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.MappedAsyncPagingIterable;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.internal.core.AsyncPagingIterableWrapper;
import com.exactpro.cradle.cassandra.dao.EntityConverter;
import com.exactpro.cradle.cassandra.retries.CannotRetryException;
import com.exactpro.cradle.cassandra.retries.PagingSupplies;
import com.exactpro.cradle.cassandra.retries.RetryUtils;

/**
 * Wrapper for asynchronous paging iterable to get entities retrieved from Cassandra
 * @param <E> - class of entities obtained from Cassandra
 */
public class PagedIterator<E> implements Iterator<E>
{
	private final Logger logger = LoggerFactory.getLogger(PagedIterator.class);
	
	private MappedAsyncPagingIterable<E> rows;
	private Iterator<E> rowsIterator;
	private final PagingSupplies pagingSupplies;
	private final Function<Row, E> mapper;
	private final String queryInfo;
	
	public PagedIterator(MappedAsyncPagingIterable<E> rows, PagingSupplies pagingSupplies, EntityConverter<E> converter, String queryInfo)
	{
		this.rows = rows;
		this.rowsIterator = rows.currentPage().iterator();
		this.pagingSupplies = pagingSupplies;
		this.mapper = converter::convert;
		this.queryInfo = queryInfo;
	}
	
	
	@Override
	public boolean hasNext()
	{
		if (rowsIterator == null || !rowsIterator.hasNext())
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
		logger.trace("Getting next data row for '{}'", queryInfo);
		return rowsIterator.next();
	}
	
	
	private Iterator<E> fetchNextIterator() throws IllegalStateException, InterruptedException, ExecutionException, CannotRetryException
	{
		if (rows.hasMorePages())
		{
			logger.debug("Getting next result page for '{}'", queryInfo);
			rows = fetchNextPage(rows);
			return rows.currentPage().iterator();
		}
		return null;
	}
	
	private MappedAsyncPagingIterable<E> fetchNextPage(MappedAsyncPagingIterable<E> rows) 
			throws CannotRetryException, IllegalStateException, InterruptedException, ExecutionException
	{
		if (pagingSupplies == null)
		{
			logger.debug("Fetching next result page for '{}' with default behavior", queryInfo);
			return rows.fetchNextPage().toCompletableFuture().get();
		}
		
		ExecutionInfo ei = rows.getExecutionInfo();
		ByteBuffer newState = ei.getPagingState();
		Statement<?> stmt = ei.getStatement().copy(newState);
		
		//Page size can be smaller than max size if RetryingSelectExecutor reduced it, so policy may restore it back
		stmt = RetryUtils.applyPolicyVerdict(stmt, pagingSupplies.getExecPolicy().onNextPage(stmt, queryInfo));
		
		CqlSession session = pagingSupplies.getSession();
		int retryCount = 0;
		do
		{
			try
			{
				return session.executeAsync(stmt).thenApply(next -> new AsyncPagingIterableWrapper<Row, E>(next, mapper))
						.toCompletableFuture().get();
			}
			catch (Exception e)
			{
				stmt = ei.getStatement().copy(newState).setPageSize(stmt.getPageSize()).setConsistencyLevel(stmt.getConsistencyLevel());
				stmt = RetryUtils.applyPolicyVerdict(stmt, pagingSupplies.getExecPolicy().onError(stmt, queryInfo, e, retryCount));
				retryCount++;
				logger.debug("Retrying next page request ({}) for '{}' with page size {} and CL {} after error: '{}'", 
						retryCount, queryInfo, stmt.getPageSize(), stmt.getConsistencyLevel(), e.getMessage());
			}
		}
		while (true);
	}
}

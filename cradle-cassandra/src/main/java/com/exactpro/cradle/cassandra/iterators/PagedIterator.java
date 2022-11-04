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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

import com.datastax.oss.driver.api.core.cql.Row;
import com.exactpro.cradle.cassandra.retries.SelectQueryExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.oss.driver.api.core.MappedAsyncPagingIterable;

/**
 * Wrapper for asynchronous paging iterable to get entities retrieved from Cassandra
 * @param <E> - class of entities obtained from Cassandra
 */
public class PagedIterator<E> implements Iterator<E> {
	private final Logger logger = LoggerFactory.getLogger(PagedIterator.class);

	private MappedAsyncPagingIterable<E> paginator;
	private final SelectQueryExecutor selectQueryExecutor;
	private final Function<Row, E> mapper;
	private final String queryInfo;
	private Iterator<E> rowsIterator;
	private CompletableFuture<MappedAsyncPagingIterable<E>> nextPage;

	private final String id;
	private final int hash;

	public PagedIterator(MappedAsyncPagingIterable<E> paginator, SelectQueryExecutor selectQueryExecutor,
						 Function<Row, E> mapper, String queryInfo)
	{
		this.hash = System.identityHashCode(this);
		this.id = Long.toHexString(hash);
		String prefix = String.format("init: [%s]", id);

		logger.trace("{} creating iterator", prefix);

		this.paginator = paginator;
		this.selectQueryExecutor = selectQueryExecutor;
		this.mapper = mapper;
		this.queryInfo = queryInfo;
		this.rowsIterator = paginator.currentPage().iterator();
		fetchNextPage();

		logger.trace("{} iterator created", prefix);
	}

	private int fetchCalls;
	private void fetchNextPage() {
		fetchCalls++;
		final String prefix = String.format("fetchNextPage: [%s-%d]", id, fetchCalls);
		logger.trace("{} enter", prefix);

		if (paginator.hasMorePages()) {
			logger.trace("{} prefetching", prefix);
			nextPage = selectQueryExecutor.fetchNextPage(paginator, mapper, queryInfo)
					.whenComplete((r, e) -> {
						if (e != null) {
							logger.error("{} Exception fetching next page", prefix, e);
						} else
							logger.trace("{} page prefetching complete", prefix, e);
					});
		} else {
			logger.trace("{} no more pages to prefetch", prefix);
			nextPage = null;
		}

		logger.trace("{} exit", prefix);
	}


	@Override
	public boolean hasNext() {
		if (rowsIterator == null)
			return false;

		if (!rowsIterator.hasNext()) {
			try {
				rowsIterator = nextIterator();
			} catch (Exception e) {
				throw new RuntimeException(String.format("Exception getting next page [%s])", id), e);
			}

			if (rowsIterator == null || !rowsIterator.hasNext())
				return false;
		}
		return true;
	}


	private int rowsFetched;
	@Override
	public E next()	{
		rowsFetched++;
		return rowsIterator.next();
	}


	private int nextCalls;
	private Iterator<E> nextIterator() throws IllegalStateException, InterruptedException, ExecutionException {
		nextCalls++;
		final String prefix = String.format("nextIterator: [%s-%d]", id, nextCalls);
		logger.trace("{} changing page after {} rows fetched", prefix, rowsFetched);

		if (nextPage != null) {
			paginator = nextPage.get();
			fetchNextPage();
			logger.trace("{} page changed", prefix);
			return paginator.currentPage().iterator();
		} else {
			logger.trace("{} no page to change to", prefix);
			return null;
		}
	}


	@Override
	public int hashCode() {
		return hash;
	}
}
/*
 * Copyright 2020-2022 Exactpro (Exactpro Systems Limited)
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

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.MappedAsyncPagingIterable;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.session.Request;
import com.exactpro.cradle.cassandra.dao.EntityConverter;
import com.exactpro.cradle.cassandra.retries.CannotRetryException;
import com.exactpro.cradle.cassandra.retries.PagingSupplies;
import com.exactpro.cradle.cassandra.retries.RetryUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

/**
 * Wrapper for asynchronous paging iterable to get entities retrieved from Cassandra
 * @param <E> - class of entities obtained from Cassandra
 */
public class PagedIterator<E> implements Iterator<E> {
	private final Logger logger = LoggerFactory.getLogger(PagedIterator.class);
	
	private MappedAsyncPagingIterable<E> paginator;
	private Iterator<E> rowsIterator;
	private final PagingSupplies pagingSupplies;
	private final Function<Row, E> mapper;
	private final String queryInfo;
	private CompletableFuture<MappedAsyncPagingIterable<E>> nextPage;

	private final String id;
	private final int hash;

	public PagedIterator(MappedAsyncPagingIterable<E> paginator, PagingSupplies pagingSupplies, EntityConverter<E> converter, String queryInfo) {

		this.hash = System.identityHashCode(this);
		this.id = Long.toHexString(hash);
		String prefix = String.format("init: [%s]", id);

		logger.trace("{} creating iterator", prefix);

		this.paginator = paginator;
		this.pagingSupplies = pagingSupplies;
		this.mapper = converter::convert;
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
			nextPage = fetchNextPage(paginator)
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


	private Statement<?> getStatement(ExecutionInfo executionInfo, String queryInfo) {
		Request request = executionInfo.getRequest();
		if (!(request instanceof Statement<?>))
			throw new IllegalStateException(String.format("The request for query \"%s\" has unsupported type \"%s\" but required \"%s\""
															, queryInfo
															, request.getClass()
															, Statement.class));
		return (Statement<?>) request;
	}



	private CompletableFuture<MappedAsyncPagingIterable<E>> execute(CqlSession session,
																	Statement<?> statement,
																	ExecutionInfo ei,
																	ByteBuffer pagingState,
																	Throwable throwable,
																	int retryCount) throws CannotRetryException {

		if (throwable != null) {
			statement = getStatement(ei, queryInfo).copy(pagingState)
													.setPageSize(statement.getPageSize())
													.setConsistencyLevel(statement.getConsistencyLevel());
			statement = RetryUtils.applyPolicyVerdict(statement, pagingSupplies.getExecPolicy()
									.onError(statement, queryInfo, throwable, retryCount));

			logger.debug("Retrying next page request ({}) for '{}' with page size {} and CL {} after error: '{}'",
								retryCount + 1,
								queryInfo,
								statement.getPageSize(),
								statement.getConsistencyLevel(),
								throwable.getMessage());
		}

		Statement<?> s = statement;
		return session.executeAsync(statement)
						.thenApplyAsync(asyncResultSet -> asyncResultSet.map(mapper))
						.handleAsync((r, t) -> {
							try {
								if (t != null) {
									return execute(session, s, ei, pagingState, t, retryCount + 1).get();
								} else {
									return r;
								}
							} catch (Exception e) {
								throw new CompletionException(e);
							}
						}).toCompletableFuture();
	}


	private CompletableFuture<MappedAsyncPagingIterable<E>> fetchNextPage(MappedAsyncPagingIterable<E> rows) {

		try {
			if (pagingSupplies == null) {
				logger.debug("Fetching next result page for '{}' with default behavior", queryInfo);
				return rows.fetchNextPage().toCompletableFuture();
			}

			ExecutionInfo executionInfo = rows.getExecutionInfo();
			ByteBuffer pagingState = executionInfo.getPagingState();
			Statement<?> statement = getStatement(executionInfo, queryInfo).copy(pagingState);

			//Page size can be smaller than max size if RetryingSelectExecutor reduced it, so policy may restore it back
			statement = RetryUtils.applyPolicyVerdict(statement, pagingSupplies.getExecPolicy()
									.onNextPage(statement, queryInfo));

			return execute(pagingSupplies.getSession(), statement, executionInfo, pagingState, null, 0);

		} catch (Exception e) {
			return CompletableFuture.failedFuture(e);
		}
	}

	@Override
	public int hashCode() {
		return hash;
	}
}
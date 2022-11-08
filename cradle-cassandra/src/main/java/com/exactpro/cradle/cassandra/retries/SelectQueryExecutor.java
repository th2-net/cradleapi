/*
 * Copyright 2021-2022 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.cradle.cassandra.retries;

import com.datastax.oss.driver.api.core.AsyncPagingIterable;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DriverException;
import com.datastax.oss.driver.api.core.MappedAsyncPagingIterable;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.session.Request;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

public class SelectQueryExecutor
{
	private static final Logger logger = LoggerFactory.getLogger(SelectQueryExecutor.class);

	private final CqlSession session;
	private final SelectExecutionPolicy multiRowResultExecPolicy, singleRowResultExecPolicy;
	private final ExecutorService composingService;

	public SelectQueryExecutor(CqlSession session, ExecutorService composingService,
			SelectExecutionPolicy multiRowResultExecPolicy, SelectExecutionPolicy singleRowResultExecPolicy)
	{
		this.session = session;
		this.composingService = composingService;
		this.multiRowResultExecPolicy = multiRowResultExecPolicy;
		this.singleRowResultExecPolicy = singleRowResultExecPolicy;
	}

	public <T> CompletableFuture<T> executeSingleRowResultQuery(Supplier<CompletableFuture<T>> query,
			Function<Row, T> mapper, String queryInfo)
	{
		CompletableFuture<T> f = new CompletableFuture<>();
		query.get()
				.whenCompleteAsync((result, error) -> onCompleteSingle(result, error, f, mapper, queryInfo, 0));
		return f;
	}


	public <T> CompletableFuture<MappedAsyncPagingIterable<T>> executeMultiRowResultQuery(
			Supplier<CompletableFuture<AsyncResultSet>> query, Function<Row, T> mapper, String queryInfo)
	{
		return executeMappedMultiRowResultQuery(
				() -> query.get().thenApplyAsync(rs -> rs.map(mapper), composingService),
				mapper, queryInfo);
	}

	public <T> CompletableFuture<MappedAsyncPagingIterable<T>> executeMappedMultiRowResultQuery(
			Supplier<CompletableFuture<MappedAsyncPagingIterable<T>>> query,
			Function<Row, T> mapper, String queryInfo)
	{
		CompletableFuture<MappedAsyncPagingIterable<T>> f = new CompletableFuture<>();
		query.get().whenCompleteAsync((result, error) -> onCompleteMulti(result, error, f, mapper, queryInfo, 0),
				composingService);
		return f;
	}

	public <T> CompletableFuture<MappedAsyncPagingIterable<T>> fetchNextPage(MappedAsyncPagingIterable<T> rows,
			Function<Row, T> mapper, String queryInfo)
	{
		ExecutionInfo ei = rows.getExecutionInfo();
		ByteBuffer newState = ei.getPagingState();
		Statement<?> statement = getStatement(ei, queryInfo);
		Statement<?> stmt = statement.copy(newState);

		//Page size can be smaller than max size if RetryingSelectExecutor reduced it, so policy may restore it back
		stmt = RetryUtils.applyPolicyVerdict(stmt, multiRowResultExecPolicy.onNextPage(stmt, queryInfo));

		CompletableFuture<MappedAsyncPagingIterable<T>> f = new CompletableFuture<>();
		session.executeAsync(stmt)
				.thenApplyAsync(rs -> rs.map(mapper), composingService)
				.whenCompleteAsync(
						(retryResult, retryError) -> onCompleteMulti(retryResult, retryError, f, mapper, queryInfo,
								0), composingService);
		return f;
	}

	private Statement<?> handleErrorAndGetStatement(Throwable error, CompletableFuture<?> f,
			SelectExecutionPolicy execPolicy, String queryInfo, int retryCount)
	{
		DriverException driverError = RetryUtils.getDriverException(error);
		if (driverError == null)
		{
			logger.error("Cannot retry '" + queryInfo + "' after non-driver exception", error);
			f.completeExceptionally(error);
			return null;
		}

		Statement<?> stmt = getStatement(driverError.getExecutionInfo(), queryInfo);
		try
		{
			return RetryUtils.applyPolicyVerdict(stmt, execPolicy.onError(stmt, queryInfo, error, retryCount));
		}
		catch (CannotRetryException e)
		{
			f.completeExceptionally(e);
			return null;
		}
	}

	private Statement<?> getStatement(ExecutionInfo executionInfo, String queryInfo)
	{
		Request request = executionInfo.getRequest();
		if (!(request instanceof Statement<?>))
			throw new IllegalStateException(
					"The request for query '" + queryInfo + "' has unsupported type '" + request.getClass() +
							"' but required '"+Statement.class+"'");
		return (Statement<?>) request;
	}

	private <T> void onCompleteSingle(T result, Throwable error, CompletableFuture<T> f, Function<Row, T> mapper,
			String queryInfo, int retryCount)
	{
		if (error == null)
		{
			logger.trace("Request ({}) completed successfully", queryInfo);
			f.complete(result);
			return;
		}

		Statement<?> stmt = handleErrorAndGetStatement(error, f, singleRowResultExecPolicy, queryInfo, retryCount);
		if (f.isDone())
			return;

		try
		{
			long delay = RetryUtils.calculateDelayWithJitter(retryCount);
			CompletableFuture.runAsync(
							() -> logger.debug("Retrying request ({}) '{}' and CL {} with delay {}ms after error: '{}'",
									retryCount + 1, queryInfo, stmt.getConsistencyLevel(), delay, error.getMessage()),
							CompletableFuture.delayedExecutor(delay, TimeUnit.MILLISECONDS))
					.thenComposeAsync(r -> session.executeAsync(stmt), composingService)
					.thenApplyAsync(AsyncPagingIterable::one, composingService)
					.thenApplyAsync(row -> row == null ? null : mapper.apply(row), composingService)
					.whenCompleteAsync((retryResult, retryError) ->
							onCompleteSingle(retryResult, retryError, f, mapper, queryInfo, retryCount+1),
							composingService);
		}
		catch (Exception e)
		{
			logger.error("Error while retrying '"+queryInfo+"'", e);
			f.completeExceptionally(e);
		}
	}

	private <T> void onCompleteMulti(MappedAsyncPagingIterable<T> result, Throwable error,
			CompletableFuture<MappedAsyncPagingIterable<T>> f, Function<Row, T> mapper,
			String queryInfo, int retryCount)
	{
		if (error == null)
		{
			logger.trace("Request ({}) completed successfully", queryInfo);
			f.complete(result);
			return;
		}

		Statement<?> stmt = handleErrorAndGetStatement(error, f, multiRowResultExecPolicy, queryInfo, retryCount);
		if (f.isDone())
			return;

		try
		{
			long delay = RetryUtils.calculateDelayWithJitter(retryCount);
			CompletableFuture.runAsync(
							() -> logger.debug("Retrying request ({}) '{}' and CL {} with delay {}ms after error: '{}'",
									retryCount + 1, queryInfo, stmt.getConsistencyLevel(), delay, error.getMessage()),
							CompletableFuture.delayedExecutor(delay, TimeUnit.MILLISECONDS))
					.thenComposeAsync(r -> session.executeAsync(stmt), composingService)
					.thenApplyAsync(row -> row.map(mapper), composingService)
					.whenCompleteAsync(
							(retryResult, retryError) -> onCompleteMulti(retryResult, retryError, f, mapper, queryInfo,
									retryCount + 1), composingService);
		}
		catch (Exception e)
		{
			logger.error("Error while retrying '" + queryInfo + "'", e);
			f.completeExceptionally(e);
		}

	}
}

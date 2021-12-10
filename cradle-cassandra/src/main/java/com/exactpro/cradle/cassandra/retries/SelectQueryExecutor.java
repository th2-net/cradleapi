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

package com.exactpro.cradle.cassandra.retries;

import com.datastax.oss.driver.api.core.*;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.internal.core.AsyncPagingIterableWrapper;
import com.exactpro.cradle.cassandra.dao.EntityConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

public class SelectQueryExecutor
{
	private static final Logger logger = LoggerFactory.getLogger(SelectQueryExecutor.class);

	private final CqlSession session;
	private final SelectExecutionPolicy multiRowResultExecPolicy, singleRowResultExecPolicy;

	public SelectQueryExecutor(CqlSession session, SelectExecutionPolicy multiRowResultExecPolicy,
			SelectExecutionPolicy singleRowResultExecPolicy)
	{
		this.session = session;
		this.multiRowResultExecPolicy = multiRowResultExecPolicy;
		this.singleRowResultExecPolicy = singleRowResultExecPolicy;
	}

	public <T> CompletableFuture<T> executeSingleRowResultQuery(Supplier<CompletableFuture<T>> query,
			EntityConverter<T> converter, String queryInfo)
	{
		CompletableFuture<T> f = new CompletableFuture<>();
		query.get().whenCompleteAsync(
				(result, error) -> onCompleteSingle(result, error, f, converter::convert, queryInfo, 0));
		return f;
	}

	public <T> CompletableFuture<MappedAsyncPagingIterable<T>> executeMultiRowResultQuery(
			Supplier<CompletableFuture<MappedAsyncPagingIterable<T>>> query,
			EntityConverter<T> converter, String queryInfo)
	{
		CompletableFuture<MappedAsyncPagingIterable<T>> f = new CompletableFuture<>();
		Function<Row, T> mapper = converter::convert;
		query.get().whenCompleteAsync((result, error) -> onCompleteMulti(result, error, f, mapper, queryInfo, 0));
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

		Statement<?> stmt = driverError.getExecutionInfo().getStatement();
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

	private <T> void onCompleteSingle(T result, Throwable error, CompletableFuture<T> f, Function<Row, T> mapper,
			String queryInfo, int retryCount)
	{
		if (error == null)
		{
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
					.thenComposeAsync(r -> session.executeAsync(stmt))
					.thenApplyAsync(AsyncPagingIterable::one)
					.thenApplyAsync(row -> row == null ? null : mapper.apply(row))
					.whenCompleteAsync((retryResult, retryError) ->
							onCompleteSingle(retryResult, retryError, f, mapper, queryInfo, retryCount+1));
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
					.thenComposeAsync(r -> session.executeAsync(stmt))
					.thenApplyAsync(row -> new AsyncPagingIterableWrapper<Row, T>(row, mapper))
					.whenCompleteAsync(
							(retryResult, retryError) -> onCompleteMulti(retryResult, retryError, f, mapper, queryInfo,
									retryCount + 1));
		}
		catch (Exception e)
		{
			logger.error("Error while retrying '" + queryInfo + "'", e);
			f.completeExceptionally(e);
		}

	}
}

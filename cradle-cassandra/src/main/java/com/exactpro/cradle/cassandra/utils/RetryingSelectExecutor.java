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

package com.exactpro.cradle.cassandra.utils;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DriverException;
import com.datastax.oss.driver.api.core.DriverTimeoutException;
import com.datastax.oss.driver.api.core.MappedAsyncPagingIterable;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.internal.core.AsyncPagingIterableWrapper;
import com.exactpro.cradle.cassandra.retries.CannotRetryException;
import com.exactpro.cradle.cassandra.retries.SelectRetryPolicy;

public class RetryingSelectExecutor
{
	private static final Logger logger = LoggerFactory.getLogger(RetryingSelectExecutor.class);
	
	private final CqlSession session;
	private final SelectRetryPolicy retryPolicy;
	
	public RetryingSelectExecutor(CqlSession session, SelectRetryPolicy retryPolicy)
	{
		this.session = session;
		this.retryPolicy = retryPolicy;
	}
	
	public <T> CompletableFuture<MappedAsyncPagingIterable<T>> executeQuery(Supplier<CompletableFuture<MappedAsyncPagingIterable<T>>> query,
			Function<Row, T> mapper)
	{
		CompletableFuture<MappedAsyncPagingIterable<T>> f = new CompletableFuture<>();
		query.get().whenCompleteAsync((result, error) -> onComplete(result, error, f, mapper));
		return f;
	}
	
	private <T> void onComplete(MappedAsyncPagingIterable<T> result, Throwable error, 
			CompletableFuture<MappedAsyncPagingIterable<T>> f, Function<Row, T> mapper)
	{
		if (error == null)
		{
			f.complete(result);
			return;
		}
		
		DriverException driverError = getDriverException(error);
		if (driverError == null)
		{
			f.completeExceptionally(error);
			return;
		}
		
		Statement<?> stmt = driverError.getExecutionInfo().getStatement();
		int newSize;
		try
		{
			newSize = retryPolicy.adjustPageSize(stmt.getPageSize(), error);
		}
		catch (CannotRetryException e)
		{
			f.completeExceptionally(e);
			return;
		}
		
		logger.debug("Retrying request with page size {} after error: '{}'", newSize, error.getMessage());
		stmt = stmt.setPageSize(newSize);
		session.executeAsync(stmt).thenApply(row -> new AsyncPagingIterableWrapper<Row, T>(row, mapper))
				.whenCompleteAsync((retryResult, retryError) -> onComplete(retryResult, retryError, f, mapper));
	}
	
	private DriverException getDriverException(Throwable e)
	{
		if (e instanceof DriverTimeoutException)
			return (DriverTimeoutException)e;
		
		Throwable cause = e.getCause();
		return cause == null || !(cause instanceof DriverTimeoutException) ? null : (DriverTimeoutException)cause;
	}
}

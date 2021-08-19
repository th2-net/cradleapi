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

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DriverException;
import com.datastax.oss.driver.api.core.MappedAsyncPagingIterable;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.internal.core.AsyncPagingIterableWrapper;
import com.exactpro.cradle.cassandra.dao.EntityConverter;

public class RetryingSelectExecutor
{
	private static final Logger logger = LoggerFactory.getLogger(RetryingSelectExecutor.class);
	
	private final CqlSession session;
	private final SelectExecutionPolicy execPolicy;
	
	public RetryingSelectExecutor(CqlSession session, SelectExecutionPolicy execPolicy)
	{
		this.session = session;
		this.execPolicy = execPolicy;
	}
	
	public <T> CompletableFuture<MappedAsyncPagingIterable<T>> executeQuery(Supplier<CompletableFuture<MappedAsyncPagingIterable<T>>> query,
			EntityConverter<T> converter, String queryInfo)
	{
		CompletableFuture<MappedAsyncPagingIterable<T>> f = new CompletableFuture<>();
		Function<Row, T> mapper = row -> converter.convert(row);
		query.get().whenCompleteAsync((result, error) -> onComplete(result, error, f, mapper, queryInfo, 0));
		return f;
	}
	
	private <T> void onComplete(MappedAsyncPagingIterable<T> result, Throwable error, 
			CompletableFuture<MappedAsyncPagingIterable<T>> f, Function<Row, T> mapper,
			String queryInfo, int retryCount)
	{
		if (error == null)
		{
			f.complete(result);
			return;
		}
		
		DriverException driverError = RetryUtils.getDriverException(error);
		if (driverError == null)
		{
			logger.error("Cannot retry '"+queryInfo+"' after non-driver exception", error);
			f.completeExceptionally(error);
			return;
		}
		
		Statement<?> stmt = driverError.getExecutionInfo().getStatement();
		try
		{
			stmt = RetryUtils.applyPolicyVerdict(stmt, execPolicy.onError(stmt, queryInfo, error, retryCount));
		}
		catch (CannotRetryException e)
		{
			f.completeExceptionally(e);
			return;
		}
		
		try
		{
			logger.debug("Retrying request ({}) '{}' with page size {} and CL {} after error: '{}'", 
					retryCount+1, queryInfo, stmt.getPageSize(), stmt.getConsistencyLevel(), error.getMessage());
			
			session.executeAsync(stmt).thenApply(row -> new AsyncPagingIterableWrapper<Row, T>(row, mapper))
					.whenCompleteAsync((retryResult, retryError) -> onComplete(retryResult, retryError, f, mapper, queryInfo, retryCount+1));
		}
		catch (Exception e)
		{
			logger.error("Error while retrying '"+queryInfo+"'", e);
			f.completeExceptionally(e);
		}
	}
}

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

package com.exactpro.cradle.cassandra.retry;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Synchronous executor for requests to Cassandra.
 * Requests are executed, blocking the current thread.
 * If request execution fails with a recoverable error, the request is retried.
 * Current thread remains blocked until request completes successfully or with unrecoverable error or if number of retries exceeds limit defined for request.
 */
public class SyncExecutor
{
	private static final Logger logger = LoggerFactory.getLogger(SyncExecutor.class);
	
	private final int defaultRetries,
			minDelay,
			maxDelay;
	
	public SyncExecutor(int defaultRetries, int minDelay, int maxDelay)
	{
		this.defaultRetries = defaultRetries;
		this.minDelay = minDelay;
		this.maxDelay = maxDelay;
	}
	
	
	public <T> T submit(String requestInfo, int maxRetries, Supplier<CompletableFuture<T>> supplier) throws Exception
	{
		RequestInfo<T> request = new RequestInfo<T>(requestInfo, maxRetries, supplier, new CompletableFuture<T>());
		do
		{
			try
			{
				return supplier.get().get();
			}
			catch (Throwable e)
			{
				logger.warn(RequestUtils.getFailureMessage(request, e));
				try
				{
					if (RequestUtils.handleRequestError(request, e))
						return request.getFuture().get();  //Error handled = future completed exceptionally
					
					request.nextRetry();
					if (!RequestUtils.sleepBeforeRetry(request, e, minDelay, maxDelay))
						return request.getFuture().get();  //Failed to make delay = future completed exceptionally
				}
				catch (ExecutionException | CompletionException e2)
				{
					Throwable cause = e2.getCause();
					if (cause != null && cause instanceof Exception)
						throw (Exception)cause;
					throw e2;
				}
				
				logger.warn(RequestUtils.getRetryMessage(request));
			}
		}
		while (!Thread.currentThread().isInterrupted());
		
		throw new RetryException("'"+requestInfo+"' failed and retry was interrupted");
	}
	
	public <T> T submit(String requestInfo, Supplier<CompletableFuture<T>> supplier) throws Exception
	{
		return submit(requestInfo, defaultRetries, supplier);
	}
}

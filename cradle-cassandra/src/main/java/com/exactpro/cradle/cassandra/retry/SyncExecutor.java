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
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SyncExecutor
{
	private static final Logger logger = LoggerFactory.getLogger(SyncExecutor.class);
	
	private final int defaultRetries,
			delay;
	
	public SyncExecutor(int defaultRetries, int delay)
	{
		this.defaultRetries = defaultRetries;
		this.delay = delay;
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
				if (RequestUtils.handleRequestError(request, e))
					return request.getFuture().get();  //Error handled = future completed exceptionally
				
				if (!RequestUtils.sleepBeforeRetry(request, e, delay))
					return request.getFuture().get();  //Failed to make delay = future completed exceptionally
				
				logger.warn(RequestUtils.getRetryMessage(request), e);
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

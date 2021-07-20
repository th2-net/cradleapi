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

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AsyncExecutor
{
	private static final Logger logger = LoggerFactory.getLogger(AsyncExecutor.class);
	
	//This queue should be unbounded else submission of a task will block
	private final BlockingQueue<RequestInfo<?>> requests = new LinkedBlockingQueue<RequestInfo<?>>();
	private final ExecutorService execService;
	private final int defaultRetries;
	private final AsyncRequestProcessor processor;
	
	public AsyncExecutor(ExecutorService execService, ExecutorService composingService, int defaultRetries, int delay)
	{
		this.execService = execService;
		this.defaultRetries = defaultRetries;
		this.processor = new AsyncRequestProcessor(requests, composingService, delay);
		execService.submit(processor);
	}
	
	public <T> CompletableFuture<T> submit(String requestInfo, int maxRetries, Supplier<CompletableFuture<T>> supplier)
	{
		CompletableFuture<T> result = new CompletableFuture<T>();
		try
		{
			requests.add(new RequestInfo<T>(requestInfo, maxRetries, supplier, result));
		}
		catch (IllegalArgumentException e)
		{
			String msg = "Could not submit new request '"+requestInfo+"'";
			logger.error(msg, e);
			result.completeExceptionally(new RetryException(msg, e));
		}
		return result;
	}
	
	public <T> CompletableFuture<T> submit(String requestInfo, Supplier<CompletableFuture<T>> supplier)
	{
		return submit(requestInfo, defaultRetries, supplier);
	}
	
	public void dispose()
	{
		execService.shutdownNow();
	}
	
	public int getActiveRequests()
	{
		return processor.getActiveRequests();
	}
	
	public int getPendingRequests()
	{
		return requests.size();
	}
}

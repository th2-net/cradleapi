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
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Supplier;

import com.exactpro.cradle.exceptions.TooManyRequestsException;

/**
 * Asynchronous executor for requests to Cassandra.
 * Requests are queued and CompletableFuture to track the execution is returned.
 * Queue size is limited. Requests that can't be queued due to this limitation are completed exceptionally.
 * If request execution fails with a recoverable error, the request is retried.
 * CompletableFuture related to the request remains incomplete until request completes successfully or with unrecoverable error
 * or if number of retries exceeds limit defined for request.
 */
public class AsyncExecutor
{
	private final BlockingQueue<RequestInfo<?>> requests;
	private final ExecutorService execService;
	private final int defaultRetries;
	private final AsyncRequestProcessor processor;
	
	public AsyncExecutor(int maxQueueSize, ExecutorService composingService, int defaultRetries, int delay)
	{
		this.requests = new LinkedBlockingQueue<RequestInfo<?>>(maxQueueSize);
		this.execService = Executors.newSingleThreadExecutor();
		this.defaultRetries = defaultRetries;
		this.processor = new AsyncRequestProcessor(requests, composingService, delay);
		
		execService.submit(processor);
	}
	
	public <T> CompletableFuture<T> submit(String requestInfo, int maxRetries, Supplier<CompletableFuture<T>> supplier) throws TooManyRequestsException
	{
		CompletableFuture<T> result = new CompletableFuture<T>();
		if (!requests.offer(new RequestInfo<T>(requestInfo, maxRetries, supplier, result)))
			throw new TooManyRequestsException("Could not submit new request '"+requestInfo+"'");
		return result;
	}
	
	public <T> CompletableFuture<T> submit(String requestInfo, Supplier<CompletableFuture<T>> supplier) throws TooManyRequestsException
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

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

import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.cradle.exceptions.TooManyRequestsException;

/**
 * Asynchronous processor for requests to Cassandra.
 * It takes requests from given queue, triggering their execution and assigning asynchronous callbacks.
 * Callback puts request back to queue if it has failed with recoverable error. 
 * If number of retries exceeds limit defined for request or the failure is unrecoverable, request completes exceptionally.
 */
public class AsyncRequestProcessor implements Runnable
{
	private static final Logger logger = LoggerFactory.getLogger(AsyncRequestProcessor.class);
	
	private final BlockingQueue<RequestInfo<?>> requests;
	private final ExecutorService composingService;
	private final int minDelay,
			maxDelay;
	private final Set<CompletableFuture<?>> futures = ConcurrentHashMap.newKeySet();
	
	public AsyncRequestProcessor(BlockingQueue<RequestInfo<?>> requests, ExecutorService composingService, int minDelay, int maxDelay)
	{
		this.requests = requests;
		this.composingService = composingService;
		this.minDelay = minDelay;
		this.maxDelay = maxDelay;
	}
	
	@Override
	public void run()
	{
		while (!Thread.currentThread().isInterrupted())
		{
			RequestInfo<?> r;
			try
			{
				r = requests.take();
			}
			catch (InterruptedException e)
			{
				Thread.currentThread().interrupt();
				logger.info("Wait for next request to process interrupted, {} active, {} pending", futures.size(), requests.size());
				cancelFutures();
				futures.clear();
				return;
			}
			
			CompletableFuture<?> f = r.getFuture();
			if (f.isDone())  //Execution was cancelled or completed in any other way
			{
				futures.remove(f);
				continue;
			}
			
			if (logger.isTraceEnabled())
				logger.trace("Executing '{}', {} more request(s) active, {} pending", r.getInfo(), futures.size(), requests.size());
			futures.add(f);
			r.getSupplier().get().whenCompleteAsync((result, error) -> retryOrComplete(r, result, error), composingService);
		}
	}
	
	public int getActiveRequests()
	{
		return futures.size();
	}
	
	
	private void retryOrComplete(RequestInfo<?> request, Object result, Throwable error)
	{
		try
		{
			if (error == null)
			{
				request.completeFuture(result);
				return;
			}
			
			logger.warn(RequestUtils.getFailureMessage(request, error));
			if (!RequestUtils.handleRequestError(request, error))
				retry(request, error);
		}
		finally
		{
			CompletableFuture<?> f = request.getFuture();
			if (f.isDone())
				futures.remove(f);
		}
	}
	
	private void retry(RequestInfo<?> request, Throwable error)
	{
		request.nextRetry();
		if (!RequestUtils.sleepBeforeRetry(request, error, minDelay, maxDelay))
			return;
		
		logger.warn(RequestUtils.getRetryMessage(request));
		try
		{
			if (!requests.offer(request, 5000, TimeUnit.MILLISECONDS))
				request.getFuture().completeExceptionally(
						new TooManyRequestsException("Could not retry request '"+request.getInfo()+"' after "+request.getRetries()+" retries", error));
		}
		catch (InterruptedException e)
		{
			Thread.currentThread().interrupt();
			request.getFuture().completeExceptionally(e);
		}
	}
	
	private void cancelFutures()
	{
		if (futures.isEmpty())
			return;
		
		Exception e = new Exception("Async execution cancelled");
		for (CompletableFuture<?> f : futures)
			f.completeExceptionally(e);  //Not canceling to be able to track who completed the future
	}
}

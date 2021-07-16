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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AsyncRequestProcessor implements Runnable
{
	private static final Logger logger = LoggerFactory.getLogger(AsyncRequestProcessor.class);
	
	private final BlockingQueue<RequestInfo<?>> requests;
	private final ExecutorService composingService;
	private final int delay;
	private final AtomicInteger active = new AtomicInteger(0);
	
	public AsyncRequestProcessor(BlockingQueue<RequestInfo<?>> requests, ExecutorService composingService, int delay)
	{
		this.requests = requests;
		this.composingService = composingService;
		this.delay = delay;
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
				logger.info("Wait for next request to process interrupted, {} active, {} pending", active.get(), requests.size());
				return;
			}
			
			if (r.getFuture().isDone())  //Execution was cancelled or completed in any other way
				continue;
			
			if (logger.isTraceEnabled())
				logger.trace("Executing '{}', {} more request(s) active, {} pending", r.getInfo(), active.get(), requests.size());
			active.incrementAndGet();
			r.getSupplier().get().whenCompleteAsync((result, error) -> retryOrComplete(r, result, error), composingService);
		}
	}
	
	public int getActiveRequests()
	{
		return active.get();
	}
	
	
	private void retryOrComplete(RequestInfo<?> request, Object result, Throwable error)
	{
		active.decrementAndGet();
		
		if (error == null)
		{
			request.completeFuture(result);
			return;
		}
		
		if (!RequestUtils.handleRequestError(request, error))
			retry(request, error);
	}
	
	private void retry(RequestInfo<?> request, Throwable error)
	{
		if (!RequestUtils.sleepBeforeRetry(request, error, delay))
			return;
		
		logger.warn(RequestUtils.getRetryMessage(request), error);
		try
		{
			requests.add(request);
		}
		catch (IllegalArgumentException e)
		{
			String msg = "Could not retry request '"+request.getInfo()+"' after "+request.getRetries()+" retries";
			e.addSuppressed(error);
			logger.error(msg, e);
			request.getFuture().completeExceptionally(new RetryException(msg, e));
		}
	}
}

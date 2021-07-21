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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.DriverException;
import com.datastax.oss.driver.api.core.DriverExecutionException;
import com.datastax.oss.driver.api.core.DriverTimeoutException;
import com.datastax.oss.driver.api.core.RequestThrottlingException;
import com.datastax.oss.driver.api.core.connection.BusyConnectionException;
import com.datastax.oss.driver.api.core.servererrors.QueryExecutionException;

public class RequestUtils
{
	private static final Logger logger = LoggerFactory.getLogger(RequestUtils.class);
	
	private static final List<Class<? extends DriverException>> RECOVERABLE_ERRORS = Collections.unmodifiableList(Arrays.asList(
			AllNodesFailedException.class,
			BusyConnectionException.class,
			QueryExecutionException.class,
			DriverExecutionException.class,
			DriverTimeoutException.class,
			RequestThrottlingException.class));
	private static final int MAX_DELAY = 300000;  //5 minutes
	
	/**
	 * Handles error of request execution, if possible. Request's future will be completed if the method has processed the failure
	 * @param request whose failure to handle
	 * @param error caused request failure
	 * @return true if error was handled and no other processing is needed, false otherwise. I.e. if false is returned, request can be retried or processed in other way
	 */
	public static boolean handleRequestError(RequestInfo<?> request, Throwable error)
	{
		if (request.getFuture().isCancelled())
		{
			handleCancelled(request, error);
			return true;
		}
		
		int maxRetries = request.getMaxRetries();
		if (maxRetries == 0)
		{
			handleNoRetry(request, error);
			return true;
		}
		
		if (maxRetries > 0 && request.getRetries() >= maxRetries)
		{
			handleRetriesExceeded(request, error, maxRetries);
			return true;
		}
		
		if (!isRecoverableError(error))
		{
			handleUnrecoverable(request, error);
			return true;
		}
		
		return false;
	}
	
	/**
	 * Checks if request failed with given error can be retried
	 * @param error to check
	 * @return true if request failed with the error can be retried
	 */
	public static boolean isRecoverableError(Throwable error)
	{
		error = getRealError(error);
		if (error == null)  //Can't recover after runtime exception with no cause
			return false;
		
		for (Class<? extends DriverException> c : RECOVERABLE_ERRORS)
		{
			if (c.isInstance(error))
				return true;
		}
		return false;
	}
	
	/**
	 * Makes delay before request retry
	 * @param request to retry
	 * @param error that caused previous request failure
	 * @param delay to make before retry
	 * @return true if delay was successfully made. If method returns false, request's future is completed with error
	 */
	public static boolean sleepBeforeRetry(RequestInfo<?> request, Throwable error, int delay)
	{
		if (delay > 0)
		{
			try
			{
				Thread.sleep(Math.max(MAX_DELAY, delay*request.getRetries()));
			}
			catch (InterruptedException e)
			{
				Thread.currentThread().interrupt();
				e.addSuppressed(error);
				handleInterruption(request, e);
				return false;
			}
		}
		return true;
	}
	
	/**
	 * Builds a message to print before retrying failed request
	 * @param request to retry
	 * @param error cause of failure
	 * @return message to print to log
	 */
	public static String getRetryMessage(RequestInfo<?> request, Throwable error)
	{
		error = getRealError(error);
		return "Retrying '"+request.getInfo()+"', attempt #"+request.nextRetry()+" after error: "+error.getClass().getName()+" "+error.getMessage();
	}
	
	
	private static void handleCancelled(RequestInfo<?> request, Throwable error)
	{
		if (logger.isDebugEnabled())
			logger.debug("'"+request.getInfo()+"' failed and was cancelled after "+request.getRetries()+" retries", error);
	}
	
	private static void handleNoRetry(RequestInfo<?> request, Throwable error)
	{
		request.getFuture().completeExceptionally(new RetryException("'"+request.getInfo()+"' failed, retrial is switched off", error));
	}
	
	private static void handleRetriesExceeded(RequestInfo<?> request, Throwable error, int retries)
	{
		request.getFuture().completeExceptionally(new RetryException("'"+request.getInfo()+"' failed, maximum number of retries done ("+retries+")", error));
	}
	
	private static void handleUnrecoverable(RequestInfo<?> request, Throwable error)
	{
		request.getFuture().completeExceptionally(new RetryException("'"+request.getInfo()+"' failed, can't recover after error", error));
	}
	
	private static void handleInterruption(RequestInfo<?> request, Throwable error)
	{
		request.getFuture().completeExceptionally(new RetryException("Sleep before retrial of '"+request.getInfo()+"' interrupted", error));
	}
	
	
	private static Throwable getRealError(Throwable error)
	{
		if (error instanceof CompletionException || error instanceof ExecutionException)
			return error.getCause();
		return error;
	}
}

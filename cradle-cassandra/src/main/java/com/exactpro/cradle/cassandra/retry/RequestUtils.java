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
	private static final List<Class<? extends DriverException>> recoverableErrors = Collections.unmodifiableList(Arrays.asList(
			AllNodesFailedException.class,
			BusyConnectionException.class,
			QueryExecutionException.class,
			DriverExecutionException.class,
			DriverTimeoutException.class,
			RequestThrottlingException.class));
	
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
			handleNonRecoverable(request, error);
			return true;
		}
		
		return false;
	}
	
	/**
	 * Checks if request failure with given error can be retried
	 * @param error to check
	 * @return true if request failed with the error can be retried
	 */
	public static boolean isRecoverableError(Throwable error)
	{
		if (error instanceof CompletionException || error instanceof ExecutionException)
		{
			error = error.getCause();
			if (error == null)  //Can't recover after runtime exception with no cause
				return false;
		}
		
		for (Class<? extends DriverException> c : recoverableErrors)
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
				Thread.sleep(delay);
			}
			catch (InterruptedException e)
			{
				Thread.currentThread().interrupt();
				logger.info("Sleep before retry of '"+request.getInfo()+"' interrupted. Request was failed with error", error);
				return false;
			}
		}
		return true;
	}
	
	/**
	 * Builds a message to print before retrying request
	 * @param request to retry
	 * @return message to print to log
	 */
	public static String getRetryMessage(RequestInfo<?> request)
	{
		return "Retrying '"+request.getInfo()+"', attempt #"+request.nextRetry()+" after error";
	}
	
	
	private static void handleCancelled(RequestInfo<?> request, Throwable error)
	{
		String msg = "'"+request.getInfo()+"' failed and was cancelled after "+request.getRetries()+" retries";
		logger.debug(msg, error);
		request.getFuture().completeExceptionally(new RetryException(msg, error));
	}
	
	private static void handleNoRetry(RequestInfo<?> request, Throwable error)
	{
		String msg = "'"+request.getInfo()+"' failed, retrying is switched off";
		logger.warn(msg, error);
		request.getFuture().completeExceptionally(new RetryException(msg, error));
	}
	
	private static void handleRetriesExceeded(RequestInfo<?> request, Throwable error, int retries)
	{
		String msg = "'"+request.getInfo()+"' failed, maximum number of retries done ("+retries+")";
		logger.warn(msg, error);
		request.getFuture().completeExceptionally(new RetryException(msg, error));
	}
	
	private static void handleNonRecoverable(RequestInfo<?> request, Throwable error)
	{
		String msg = "'"+request.getInfo()+"' failed, can't recover after error";
		logger.warn(msg, error);
		request.getFuture().completeExceptionally(new RetryException(msg, error));
	}
}

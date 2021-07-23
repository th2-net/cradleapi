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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.exactpro.cradle.exceptions.TooManyRequestsException;

public class AsyncExecutorTest
{
	private static final String REQUEST_NAME = "fake request";
	
	private ExecutorService composingService;
	private AsyncExecutor executor;
	
	@BeforeTest
	public void initTest()
	{
		composingService = Executors.newSingleThreadExecutor();
	}
	
	@AfterTest
	public void disposeTest()
	{
		composingService.shutdownNow();
	}
	
	@BeforeMethod
	public void init()
	{
		executor = new AsyncExecutor(1, composingService, 0, 10, 10);
	}
	
	@AfterMethod
	public void dispose()
	{
		executor.dispose();
	}
	
	
	@Test
	public void retry() throws InterruptedException, ExecutionException, TooManyRequestsException
	{
		CompletableFuture<Integer> f = submitRequest(new FakeRequest(2), 10);
		Assert.assertEquals(f.get().intValue(), 2, "made 2 calls retrying failed request");
	}
	
	@Test(expectedExceptions = ExecutionException.class, expectedExceptionsMessageRegExp = ".*\\.RetryException.* retrial is switched off")
	public void noRetry() throws InterruptedException, ExecutionException, TooManyRequestsException
	{
		submitRequest(new FakeRequest(10), 0).get();
	}
	
	@Test(expectedExceptions = ExecutionException.class, expectedExceptionsMessageRegExp = ".*\\.RetryException.* maximum number of retries done .*")
	public void limitedRetries() throws InterruptedException, ExecutionException, TooManyRequestsException
	{
		submitRequest(new FakeRequest(3), 1).get();
	}
	
	@Test(expectedExceptions = ExecutionException.class, expectedExceptionsMessageRegExp = ".*\\.RetryException.* can't recover after error")
	public void unrecoverableError() throws InterruptedException, ExecutionException, TooManyRequestsException
	{
		submitRequest(new FakeRequest(2, new Exception("Can't recover")), 100).get();
	}
	
	@Test(expectedExceptions = TooManyRequestsException.class)
	public void tooManyRequests() throws TooManyRequestsException
	{
		for (int i = 0; i < 10; i++)
			submitRequest(new FakeRequest(-1), -1);
	}
	
	@Test(description = "Futures left after AsyncExecutor disposal should be completed anyway",
			expectedExceptions = ExecutionException.class, expectedExceptionsMessageRegExp = ".* cancelled")
	public void executorDispose() throws InterruptedException, ExecutionException, TooManyRequestsException
	{
		FakeRequest r = new FakeRequest(-1);
		CompletableFuture<Integer> f = submitRequest(r, -1);
		
		while (r.getCounter() < 3)
			Thread.sleep(1);
		executor.dispose();
		f.get();
	}
	
	
	private CompletableFuture<Integer> submitRequest(FakeRequest request, int retries) throws TooManyRequestsException
	{
		return executor.submit(REQUEST_NAME, retries, () -> request.call());
	}
}

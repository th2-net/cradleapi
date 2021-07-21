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

import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class SyncExecutorTest
{
	private static final String REQUEST_NAME = "fake request";
	
	private SyncExecutor executor;
	
	@BeforeTest
	public void init()
	{
		executor = new SyncExecutor(0, 10);
	}
	
	@Test
	public void retry() throws Exception
	{
		int result = submitRequest(new FakeRequest(2), 10);
		Assert.assertEquals(result, 2, "made 2 calls retrying failed request");
	}
	
	@Test(expectedExceptions = RetryException.class, expectedExceptionsMessageRegExp = ".* retrial is switched off")
	public void noRetry() throws Exception
	{
		submitRequest(new FakeRequest(10), 0);
	}
	
	@Test(expectedExceptions = RetryException.class, expectedExceptionsMessageRegExp = ".* maximum number of retries done .*")
	public void limitedRetries() throws Exception
	{
		submitRequest(new FakeRequest(3), 1);
	}
	
	@Test(expectedExceptions = RetryException.class, expectedExceptionsMessageRegExp = ".* can't recover after error")
	public void unrecoverableError() throws Exception
	{
		submitRequest(new FakeRequest(2, new Exception("Can't recover")), 100);
	}
	
	
	private int submitRequest(FakeRequest request, int retries) throws Exception
	{
		return executor.submit(REQUEST_NAME, retries, () -> request.call());
	}
}

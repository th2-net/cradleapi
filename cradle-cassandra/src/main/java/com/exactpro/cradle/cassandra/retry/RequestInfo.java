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

/**
 * Information about request to be sent to Cassandra
 * @param <T> class of request result
 */
public class RequestInfo<T>
{
	private final String info;
	private final int maxRetries;
	private final Supplier<CompletableFuture<T>> supplier;
	private final CompletableFuture<T> future;
	private volatile long retries = 0;
	
	public RequestInfo(String info, int maxRetries, Supplier<CompletableFuture<T>> supplier, CompletableFuture<T> future)
	{
		this.info = info;
		this.maxRetries = maxRetries;
		this.supplier = supplier;
		this.future = future;
	}
	
	
	public String getInfo()
	{
		return info;
	}
	
	public int getMaxRetries()
	{
		return maxRetries;
	}
	
	public Supplier<CompletableFuture<T>> getSupplier()
	{
		return supplier;
	}
	
	
	public CompletableFuture<T> getFuture()
	{
		return future;
	}
	
	public void completeFuture(Object result)
	{
		future.complete((T)result);
	}
	
	
	public long getRetries()
	{
		return retries;
	}
	
	public long nextRetry()
	{
		return ++retries;
	}
}

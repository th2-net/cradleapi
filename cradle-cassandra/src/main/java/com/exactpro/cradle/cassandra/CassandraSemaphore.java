/*
 * Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.cradle.cassandra;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Semaphore;

public class CassandraSemaphore
{
	private static final Logger logger = LoggerFactory.getLogger(CassandraSemaphore.class);
	
	private final int maxParallelQueries;
	private final Semaphore semaphore;
	
	public CassandraSemaphore(int maxParallelQueries)
	{
		this.maxParallelQueries = maxParallelQueries;
		this.semaphore = new Semaphore(maxParallelQueries, true);
	}
	
	
	public void acquireSemaphore() throws InterruptedException
	{
		logger.debug("Trying to acquire semaphore. Available permits: {}", semaphore.availablePermits());
		semaphore.acquire();
	}
	
	public void releaseSemaphore()
	{
		semaphore.release();
		logger.debug("Semaphore released. Available permits: {}", semaphore.availablePermits());
	}
	
	
	public int getMaxQueriesNumber()
	{
		return maxParallelQueries;
	}
	
	public int getAquiredQueriesNumber() throws InterruptedException
	{
		return maxParallelQueries-semaphore.availablePermits();
	}
}

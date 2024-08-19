/*
 * Copyright 2021-2024 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.cradle.cassandra.retries;

import java.util.concurrent.ThreadLocalRandom;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.DriverException;
import com.datastax.oss.driver.api.core.DriverTimeoutException;
import com.datastax.oss.driver.api.core.connection.FrameTooLongException;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.servererrors.ReadTimeoutException;
import com.datastax.oss.driver.api.core.servererrors.WriteTimeoutException;

public class RetryUtils
{
	private static final long BASE_DELAYS_MS = 1_000;
	private static final long MAX_DELAYS_MS = 10_000;

	public static DriverException getDriverException(Throwable e)
	{
		if (e instanceof DriverException)
			return (DriverException)e;
		
		Throwable cause = e.getCause();
		return cause == null ? null : getDriverException(cause);
	}
	
	public static boolean isRetriableException(Throwable e)
	{
		if (e instanceof DriverTimeoutException ||
				e instanceof FrameTooLongException ||
				e instanceof ReadTimeoutException ||
				e instanceof WriteTimeoutException)
			return true;
		
		Throwable cause = e.getCause();
		return cause != null && isRetriableException(cause);
	}
	
	public static Statement<?> applyPolicyVerdict(Statement<?> stmt, SelectExecutionVerdict policyVerdict)
	{
		if (policyVerdict == null)
			return stmt;
		
		ConsistencyLevel cl = policyVerdict.getConsistencyLevel();
		if (cl != null)
			stmt = stmt.setConsistencyLevel(cl);
		
		int pageSize = policyVerdict.getPageSize();
		if (pageSize > 0)
			stmt = stmt.setPageSize(pageSize);
		return stmt;
	}
	
	public static long calculateDelayWithJitter(int retryCount) {
		// get the pure exponential delay based on the attempt count
		long delay = Math.min(BASE_DELAYS_MS * (1L << retryCount), MAX_DELAYS_MS);
		// calculate up to 15% jitter, plus or minus (i.e. 85 - 115% of the pure value)
		int jitter = ThreadLocalRandom.current().nextInt(85, 116);
		// apply jitter
		delay = (jitter * delay) / 100;
		// ensure the final delay is between the base and max
		delay = Math.min(MAX_DELAYS_MS, Math.max(BASE_DELAYS_MS, delay));
		return delay;
	}

}

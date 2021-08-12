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

package com.exactpro.cradle.cassandra.retries;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.DriverException;
import com.datastax.oss.driver.api.core.DriverTimeoutException;
import com.datastax.oss.driver.api.core.connection.FrameTooLongException;
import com.datastax.oss.driver.api.core.cql.Statement;

public class RetryUtils
{
	public static DriverException getDriverException(Throwable e)
	{
		if (e instanceof DriverException)
			return (DriverException)e;
		
		Throwable cause = e.getCause();
		return cause == null || !(cause instanceof DriverException) ? null : (DriverException)cause;
	}
	
	public static boolean isRetriableException(Throwable e)
	{
		if (e instanceof DriverTimeoutException || e instanceof FrameTooLongException)
			return true;
		
		Throwable cause = e.getCause();
		return cause != null && (cause instanceof DriverTimeoutException || cause instanceof FrameTooLongException);
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
}

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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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
		return cause == null ? null : getDriverException(cause);
	}
	
	public static boolean isRetriableException(Throwable e)
	{
		if (e instanceof DriverTimeoutException || e instanceof FrameTooLongException)
			return true;
		
		Throwable cause = e.getCause();
		return cause == null ? false : isRetriableException(cause);
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
	
	public static List<List<String>> applyPolicyVerdict(List<String> ids, SelectExecutionVerdict policyVerdict)
	{
		if (policyVerdict == null)
			return Collections.singletonList(ids);
		
		int pageSize = policyVerdict.getPageSize();
		if (pageSize <= 0)
			return Collections.singletonList(ids);
		
		List<List<String>> result = new ArrayList<>();
		
		int length = ids.size();
		for (int i = 0; i < length/pageSize; i++)
		{
			int index = i*pageSize;
			result.add(ids.subList(index, index+pageSize));
		}
		
		int tail = length%pageSize;
		if (tail > 0)
			result.add(ids.subList(length-tail, length));
		
		return result;
	}
}

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

import java.util.Collection;

import com.datastax.oss.driver.api.core.cql.Statement;

public class NoRetryPolicy implements SelectExecutionPolicy
{
	@Override
	public SelectExecutionVerdict onError(Statement<?> statement, String queryInfo, Throwable cause, int retryCount)
			throws CannotRetryException
	{
		throw noRetries(cause);
	}

	@Override
	public SelectExecutionVerdict onNextPage(Statement<?> statement, String queryInfo)
	{
		return null;
	}
	
	
	private CannotRetryException noRetries(Throwable cause)
	{
		return new CannotRetryException("Retries are not allowed", cause);
	}
}

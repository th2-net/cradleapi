/*
 * Copyright 2021-2022 Exactpro (Exactpro Systems Limited)
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

import com.datastax.oss.driver.api.core.cql.Statement;

import java.util.Collection;

public class FixedNumberRetryPolicy implements SelectExecutionPolicy
{
	private int maxRetry;

	public FixedNumberRetryPolicy(int maxRetry)
	{
		this.maxRetry = maxRetry;
	}

	@Override
	public SelectExecutionVerdict onError(Statement<?> statement, String queryInfo, Throwable cause, int retryCount)
			throws CannotRetryException
	{
		return passVerdict(cause, retryCount, statement.getPageSize());
	}

	@Override
	public SelectExecutionVerdict onNextPage(Statement<?> statement, String queryInfo)
	{
		return new SelectExecutionVerdict(null, statement.getPageSize());
	}

	private SelectExecutionVerdict passVerdict(Throwable cause, int retryCount, int pageSize) throws CannotRetryException
	{
		if (!RetryUtils.isRetriableException(cause))
			throw new CannotRetryException("Cannot retry after this error", cause);

		if (retryCount > maxRetry)
			throw new CannotRetryException("The maximum number '" + maxRetry + "' of retries has been reached", cause);
		return new SelectExecutionVerdict(null, pageSize);
	}
}

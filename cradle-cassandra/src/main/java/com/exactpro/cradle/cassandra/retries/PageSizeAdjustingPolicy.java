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

/**
 * {@link SelectExecutionPolicy} implementation that adjusts requested page size.
 * Page size is divided by given factor in case of failure.
 * Page size is multiplied by given factor before requesting next result page.
 */
public class PageSizeAdjustingPolicy implements SelectExecutionPolicy
{
	private final int maxPageSize;
	private final int factor;
	
	public PageSizeAdjustingPolicy(int maxPageSize, int factor)
	{
		this.maxPageSize = maxPageSize;
		this.factor = factor;
		
		if (maxPageSize < 1)
			throw new IllegalArgumentException("Maximum page size cannot be less than 1");
		if (factor < 1)
			throw new IllegalArgumentException("Factor cannot be less than 1");
	}
	
	@Override
	public SelectExecutionVerdict onError(Statement<?> statement, String queryInfo, Throwable cause, int retryCount)
			throws CannotRetryException
	{
		if (!RetryUtils.isRetriableException(cause))
			throw new CannotRetryException("Cannot retry after this error", cause);
		
		int pageSize = statement.getPageSize();
		if (pageSize <= factor)
			throw new CannotRetryException("Page size is already too small ("+pageSize+"), cannot adjust it by dividing by "+factor, cause);
		return new SelectExecutionVerdict(null, pageSize / factor);
	}
	
	@Override
	public SelectExecutionVerdict onError(Collection<String> ids, String queryInfo, Throwable cause, int retryCount)
			throws CannotRetryException
	{
		if (!RetryUtils.isRetriableException(cause))
			throw new CannotRetryException("Cannot retry after this error", cause);
		
		int divider = (retryCount+1)*factor;
		if (ids.size() <= divider)
			throw new CannotRetryException("List size is already too small ("+ids.size()+"), cannot adjust it by dividing by "+divider, cause);
		return new SelectExecutionVerdict(null, ids.size() / divider);
	}
	
	@Override
	public SelectExecutionVerdict onNextPage(Statement<?> statement, String queryInfo)
	{
		int pageSize = statement.getPageSize();
		if (pageSize < maxPageSize)
		{
			pageSize *= factor;
			if (pageSize > maxPageSize)
				pageSize = maxPageSize;
		}
		return new SelectExecutionVerdict(null, pageSize);
	}
	
	
	public int getMaxPageSize()
	{
		return maxPageSize;
	}
	
	public int getFactor()
	{
		return factor;
	}
}

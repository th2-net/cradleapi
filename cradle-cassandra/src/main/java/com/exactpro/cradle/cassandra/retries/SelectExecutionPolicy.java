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

/**
 * Interface to implement policy for "select" queries execution.
 * The policy defines behavior for retries in case of errors and for result paging
 */
public interface SelectExecutionPolicy
{
	/**
	 * Defines behavior in case of error
	 * @param statement whose execution caused the error
	 * @param queryInfo description of query being executed
	 * @param cause error that failed the query
	 * @param retryCount number of retries already made
	 * @return object with changes for statement execution to perform a retry
	 * @throws CannotRetryException if retry cannot be performed
	 */
	SelectExecutionVerdict onError(Statement<?> statement, String queryInfo, Throwable cause, int retryCount) throws CannotRetryException;
	/**
	 * Defines behavior to fetch next result page
	 * @param statement whose execution will fetch next result page
	 * @param queryInfo description of query being executed
	 * @return object with changes for statement execution to fetch next page
	 */
	SelectExecutionVerdict onNextPage(Statement<?> statement, String queryInfo);
}

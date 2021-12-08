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

package com.datastax.oss.driver.internal.core.retry;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.retry.RetryDecision;
import com.datastax.oss.driver.api.core.servererrors.CoordinatorException;
import com.datastax.oss.driver.api.core.servererrors.WriteType;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.internal.core.retry.DefaultRetryPolicy;
import edu.umd.cs.findbugs.annotations.NonNull;

public class CustomRetryPolicy extends DefaultRetryPolicy
{
	public CustomRetryPolicy(DriverContext context, String profileName)
	{
		super(context, profileName);
	}

	@Override
	public RetryDecision onReadTimeout(@NonNull Request request, @NonNull ConsistencyLevel cl, int blockFor,
			int received, boolean dataPresent, int retryCount)
	{
		return super.onReadTimeout(request, cl, blockFor, received, dataPresent, retryCount);
	}

	@Override
	public RetryDecision onWriteTimeout(@NonNull Request request, @NonNull ConsistencyLevel cl,
			@NonNull WriteType writeType, int blockFor, int received, int retryCount)
	{
		return super.onWriteTimeout(request, cl, writeType, blockFor, received, retryCount);
	}

	@Override
	public RetryDecision onUnavailable(@NonNull Request request, @NonNull ConsistencyLevel cl, int required, int alive,
			int retryCount)
	{
		return super.onUnavailable(request, cl, required, alive, retryCount);
	}

	@Override
	public RetryDecision onRequestAborted(@NonNull Request request, @NonNull Throwable error, int retryCount)
	{
		return super.onRequestAborted(request, error, retryCount);
	}

	@Override
	public RetryDecision onErrorResponse(@NonNull Request request, @NonNull CoordinatorException error, int retryCount)
	{
		return super.onErrorResponse(request, error, retryCount);
	}
}

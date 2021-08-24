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

package com.exactpro.cradle.cassandra.dao;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;

import com.datastax.oss.driver.api.core.MappedAsyncPagingIterable;

public class DaoUtils
{
	public static <T> Collection<T> toCollection(MappedAsyncPagingIterable<T> resultSet) throws IllegalStateException, InterruptedException, ExecutionException
	{
		List<T> result = new ArrayList<>();
		do
		{
			for (T row : resultSet.currentPage())
				result.add(row);
			
			if (!resultSet.hasMorePages())
				break;
			
			resultSet = resultSet.fetchNextPage().toCompletableFuture().get();
		}
		while (true);
		return result;
	}
}

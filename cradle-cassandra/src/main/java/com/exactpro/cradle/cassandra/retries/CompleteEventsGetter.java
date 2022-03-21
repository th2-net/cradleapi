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
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.exactpro.cradle.CradleObjectsFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.oss.driver.api.core.DriverException;
import com.datastax.oss.driver.api.core.MappedAsyncPagingIterable;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.exactpro.cradle.cassandra.dao.testevents.TestEventEntity;
import com.exactpro.cradle.cassandra.dao.testevents.TestEventOperator;
import com.exactpro.cradle.cassandra.dao.testevents.converters.TestEventConverter;
import com.exactpro.cradle.cassandra.iterators.TestEventDataIteratorAdapter;
import com.exactpro.cradle.testevents.StoredTestEventId;
import com.exactpro.cradle.testevents.StoredTestEventWrapper;

public class CompleteEventsGetter
{
	private static final Logger logger = LoggerFactory.getLogger(CompleteEventsGetter.class);
	
	private final UUID instanceId;
	private final Function<BoundStatementBuilder, BoundStatementBuilder> readAttrs;
	private final SelectExecutionPolicy execPolicy;
	private final TestEventOperator operator;
	private final TestEventConverter converter;
	private final PagingSupplies pagingSupplies;
	private final CradleObjectsFactory objectsFactory;
	
	public CompleteEventsGetter(UUID instanceId, Function<BoundStatementBuilder, BoundStatementBuilder> readAttrs,
			SelectExecutionPolicy execPolicy, CradleObjectsFactory objectsFactory, TestEventOperator operator,
			TestEventConverter converter, PagingSupplies pagingSupplies)
	{
		this.instanceId = instanceId;
		this.readAttrs = readAttrs;
		this.execPolicy = execPolicy;
		this.objectsFactory = objectsFactory;
		this.operator = operator;
		this.converter = converter;
		this.pagingSupplies = pagingSupplies;
	}
	
	public CompletableFuture<Iterable<StoredTestEventWrapper>> get(Collection<StoredTestEventId> ids, String queryInfo)
	{
		CompletableFuture<Iterable<StoredTestEventWrapper>> f = new CompletableFuture<>();
		List<String> idStrings = ids.stream().map(StoredTestEventId::toString).collect(Collectors.toList());
		operator.getComplete(instanceId, idStrings, readAttrs)
				.thenApplyAsync(r -> toCollection(r, queryInfo))
				.whenCompleteAsync((result, error) -> onComplete(result, error, idStrings, f, queryInfo, 0));
		return f;
	}
	
	
	private Collection<StoredTestEventWrapper> toCollection(MappedAsyncPagingIterable<TestEventEntity> rs, String queryInfo)
	{
		Collection<StoredTestEventWrapper> result = new ArrayList<>();
		new TestEventDataIteratorAdapter(rs, objectsFactory, pagingSupplies, converter, queryInfo).forEach(result::add);
		return result;
	}
	
	private void onComplete(Iterable<StoredTestEventWrapper> result, Throwable error, List<String> ids, 
			CompletableFuture<Iterable<StoredTestEventWrapper>> f, String queryInfo, int retryCount)
	{
		if (error == null)
		{
			f.complete(result);
			return;
		}
		
		DriverException driverException = RetryUtils.getDriverException(error);
		if (driverException == null)
		{
			logger.error("Cannot retry '"+queryInfo+"' after non-driver exception", error);
			f.completeExceptionally(error);
			return;
		}
		
		List<List<String>> splitIds;
		try
		{
			splitIds = RetryUtils.applyPolicyVerdict(ids, execPolicy.onError(ids, queryInfo, error, retryCount));
		}
		catch (CannotRetryException e)
		{
			f.completeExceptionally(e);
			return;
		}
		
		List<StoredTestEventWrapper> resultList = new ArrayList<>();
		CompletableFuture<Iterable<StoredTestEventWrapper>> newResult = CompletableFuture.completedFuture(resultList);
		try
		{
			logger.debug("Retrying request ({}) '{}' with IDs list split into {} parts after error: '{}'", 
					retryCount+1, queryInfo, splitIds.size(), error.getMessage());
			
			for (List<String> part : splitIds)
			{
				newResult = newResult.thenComposeAsync(r -> operator.getComplete(instanceId, part, readAttrs))
						.thenApplyAsync(rs -> {
							resultList.addAll(toCollection(rs, queryInfo));
							return resultList;
						});
			}
			newResult.whenCompleteAsync((r, e) -> onComplete(r, e, ids, f, queryInfo, retryCount+1));
		}
		catch (Exception e)
		{
			logger.error("Error while retrying '"+queryInfo+"'", e);
			f.completeExceptionally(e);
		}
	}
}

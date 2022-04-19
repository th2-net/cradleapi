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

package com.exactpro.cradle.cassandra.dao.messages;

import com.datastax.oss.driver.api.core.MappedAsyncPagingIterable;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.annotations.Insert;
import com.datastax.oss.driver.api.mapper.annotations.QueryProvider;
import com.exactpro.cradle.cassandra.dao.CommonQueryProvider;
import com.exactpro.cradle.cassandra.retries.SelectQueryExecutor;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

@Dao
public interface GroupedMessageBatchOperator
{
	@QueryProvider(providerClass = CommonQueryProvider.class, entityHelpers = GroupedMessageBatchEntity.class)
	CompletableFuture<MappedAsyncPagingIterable<GroupedMessageBatchEntity>> getByFilter(CassandraGroupedMessageFilter filter,
			SelectQueryExecutor selectExecutor, String queryInfo,
			Function<BoundStatementBuilder, BoundStatementBuilder> attributes);
	
	@Insert
	CompletableFuture<Void> write(GroupedMessageBatchEntity message,
			Function<BoundStatementBuilder, BoundStatementBuilder> attributes);

}

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

package com.exactpro.cradle.cassandra.dao.messages;

import com.datastax.oss.driver.api.core.PagingIterable;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.mapper.annotations.*;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static com.exactpro.cradle.cassandra.StorageConstants.*;

@Dao
public interface PageSessionsOperator
{
	@Select
	PagingIterable<PageSessionEntity> get(String page,
			Function<BoundStatementBuilder, BoundStatementBuilder> attributes);
	
	@Query("SELECT * FROM ${qualifiedTableId} WHERE " + PAGE + "=:page AND " + SESSION_ALIAS + "=:sessionAlias AND " +
	DIRECTION + "=:direction ORDER BY " + SESSION_ALIAS + " DESC LIMIT 1")
	CompletableFuture<PageSessionEntity> getLast(String page, String sessionAlias, String direction,
			Function<BoundStatementBuilder, BoundStatementBuilder> attributes);
	
	@Insert
	CompletableFuture<PageSessionEntity> write(PageSessionEntity entity,
			Function<BoundStatementBuilder, BoundStatementBuilder> attributes);
	
	@Delete(entityClass = PageSessionEntity.class)
	void remove(String page, Function<BoundStatementBuilder, BoundStatementBuilder> attributes);
}

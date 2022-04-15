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

package com.exactpro.cradle.cassandra.dao.books;

import java.util.function.Function;

import com.datastax.oss.driver.api.core.PagingIterable;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.mapper.annotations.*;

@Dao
public interface PageNameOperator
{
	@Select
	PagingIterable<PageNameEntity> get (String part, String name);

	@Insert(ifNotExists = true)
	ResultSet writeNew(PageNameEntity entity, Function<BoundStatementBuilder, BoundStatementBuilder> attributes);
	
	@Update
	ResultSet update(PageNameEntity entity, Function<BoundStatementBuilder, BoundStatementBuilder> attributes);
	
	@Delete(entityClass = PageNameEntity.class)
	void remove(String part, String name, Function<BoundStatementBuilder, BoundStatementBuilder> attributes);
}

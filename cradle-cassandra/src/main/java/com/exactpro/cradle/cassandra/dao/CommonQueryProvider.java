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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.MappedAsyncPagingIterable;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.mapper.MapperContext;
import com.datastax.oss.driver.api.mapper.entity.EntityHelper;
import com.datastax.oss.driver.api.querybuilder.select.Select;

public class CommonQueryProvider<T>
{
	private final CqlSession session;
	private final EntityHelper<T> helper;
	
	public CommonQueryProvider(MapperContext context, EntityHelper<T> helper)
	{
		this.session = context.getSession();
		this.helper = helper;
	}
	
	public CompletableFuture<MappedAsyncPagingIterable<T>> getByFilter(CassandraFilter<T> filter,
			ExecutorService composingService,
			Function<BoundStatementBuilder, BoundStatementBuilder> attributes)
	{
		Select select = createQuery(filter);
		PreparedStatement ps = session.prepare(select.build());
		BoundStatement bs = bindParameters(ps, filter, attributes);
		return session.executeAsync(bs).thenApplyAsync(r -> r.map(helper::get), composingService).toCompletableFuture();
	}
	
	
	private Select createQuery(CassandraFilter<T> filter)
	{
		Select select = helper.selectStart();
		if (filter != null)
			select = filter.addConditions(select);
		return select;
	}
	
	private BoundStatement bindParameters(PreparedStatement ps, CassandraFilter<T> filter, 
			Function<BoundStatementBuilder, BoundStatementBuilder> attributes)
	{
		BoundStatementBuilder builder = ps.boundStatementBuilder();
		builder = attributes.apply(builder);
		if (filter != null)
			builder = filter.bindParameters(builder);
		return builder.build();
	}
}

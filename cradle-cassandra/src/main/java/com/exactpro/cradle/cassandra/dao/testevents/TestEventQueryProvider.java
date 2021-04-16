/*
 * Copyright 2020-2021 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.cradle.cassandra.dao.testevents;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.MappedAsyncPagingIterable;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.mapper.MapperContext;
import com.datastax.oss.driver.api.mapper.entity.EntityHelper;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;
import static com.exactpro.cradle.cassandra.StorageConstants.ID;
import static com.exactpro.cradle.cassandra.StorageConstants.INSTANCE_ID;

public class TestEventQueryProvider
{
	private static final Logger logger = LoggerFactory.getLogger(TestEventQueryProvider.class);

	private final CqlSession session;
	private final EntityHelper<TestEventEntity> helper;
	private final SimpleStatement ss;
	
	public TestEventQueryProvider(MapperContext context, EntityHelper<TestEventEntity> helper)
	{
		this.session = context.getSession();
		this.helper = helper;
		this.ss = helper.selectStart()
				.whereColumn(INSTANCE_ID).isEqualTo(bindMarker())
				.whereColumn(ID).in(bindMarker())
				.build();
	}
	
	public CompletableFuture<MappedAsyncPagingIterable<TestEventEntity>> getComplete(UUID instanceId, Set<String> id,
			Function<BoundStatementBuilder, BoundStatementBuilder> attributes)
	{
		BoundStatement bs = session.prepare(ss)
				.boundStatementBuilder()
				.setUuid(0, instanceId)
				.setSet(1, id, String.class)
				.build();
		return session.executeAsync(bs).thenApply(result -> result.map(helper::get)).toCompletableFuture();
	}
}

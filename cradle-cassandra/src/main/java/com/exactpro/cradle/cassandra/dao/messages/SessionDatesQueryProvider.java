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

import com.datastax.oss.driver.api.core.AsyncPagingIterable;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.mapper.MapperContext;
import com.datastax.oss.driver.api.mapper.entity.EntityHelper;

import java.time.LocalDate;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;
import static com.exactpro.cradle.cassandra.StorageConstants.*;

public class SessionDatesQueryProvider
{
	private final CqlSession session;
	private final SimpleStatement lastPartStatement;

	public SessionDatesQueryProvider(MapperContext context, EntityHelper<SessionDatesEntity> helper)
	{
		this.session = context.getSession();
		this.lastPartStatement = helper.selectStart().whereColumn(PAGE).isEqualTo(bindMarker())
				.whereColumn(MESSAGE_DATE).in(bindMarker())
				.whereColumn(SESSION_ALIAS).isEqualTo(bindMarker())
				.whereColumn(DIRECTION).isEqualTo(bindMarker())
				.limit(1)
				.build();
	}

	CompletableFuture<String> getLastPart(String page, List<LocalDate> messagesDates, String sessionAlias, String direction,
			Function<BoundStatementBuilder, BoundStatementBuilder> attributes)
	{
		BoundStatementBuilder builder = attributes.apply(session.prepare(lastPartStatement).boundStatementBuilder())
				.setString(0, page)
				.setList(1, messagesDates, LocalDate.class)
				.setString(2, sessionAlias)
				.setString(3, direction);
		return session.executeAsync(builder.build())
				.thenApply(AsyncPagingIterable::one)
				.thenApply(result -> result == null ? null : result.getString(PART))
				.toCompletableFuture();
	}
}

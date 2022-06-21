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

package com.exactpro.cradle.cassandra.workers;

import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.exactpro.cradle.BookCache;
import com.exactpro.cradle.BookId;
import com.exactpro.cradle.BookInfo;
import com.exactpro.cradle.FetchParameters;
import com.exactpro.cradle.cassandra.CassandraStorageSettings;
import com.exactpro.cradle.cassandra.dao.BookOperators;
import com.exactpro.cradle.cassandra.retries.SelectQueryExecutor;
import com.exactpro.cradle.utils.CradleStorageException;

import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

public abstract class Worker {
	// Metric Labels
	public static final String BOOK_ID = "book_id";
	public static final String SESSION_ALIAS = "session_alias";
	public static final String SCOPE = "scope";
	public static final String DIRECTION = "direction";

	protected final CassandraStorageSettings settings;
	private final BookOperators operators;
	protected final ExecutorService composingService;
	protected final BookCache bookCache;
	protected final SelectQueryExecutor selectQueryExecutor;
	protected final Function<BoundStatementBuilder, BoundStatementBuilder> writeAttrs,
			readAttrs;

	public Worker(WorkerSupplies workerSupplies) {
		this.settings = workerSupplies.getSettings();
		this.operators = workerSupplies.getOperators();
		this.composingService = workerSupplies.getComposingService();
		this.bookCache = workerSupplies.getBookCache();
		this.selectQueryExecutor = workerSupplies.getSelectExecutor();
		this.writeAttrs = workerSupplies.getWriteAttrs();
		this.readAttrs = workerSupplies.getReadAttrs();
	}

	protected BookOperators getOperators() {
		return operators;
	}

	protected BookInfo getBook(BookId bookId) throws CradleStorageException	{
		return bookCache.getBook(bookId);
	}

	protected Function<BoundStatementBuilder, BoundStatementBuilder> composeReadAttrs(FetchParameters fetchParams) {
		if (fetchParams == null)
			return readAttrs;

		int fetchSize = fetchParams.getFetchSize();
		long timeout = fetchParams.getTimeout();
		return readAttrs.andThen(builder -> fetchSize > 0 ? builder.setPageSize(fetchSize) : builder)
				.andThen(builder -> timeout > 0 ? builder.setTimeout(Duration.ofMillis(timeout)) : builder);
	}
}

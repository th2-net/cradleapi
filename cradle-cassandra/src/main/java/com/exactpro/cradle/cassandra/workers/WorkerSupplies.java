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
import com.exactpro.cradle.BookAndPageChecker;
import com.exactpro.cradle.cassandra.CassandraStorageSettings;
import com.exactpro.cradle.cassandra.dao.CradleOperators;
import com.exactpro.cradle.cassandra.retries.SelectQueryExecutor;

import java.util.concurrent.ExecutorService;
import java.util.function.Function;

public class WorkerSupplies
{
	private final CassandraStorageSettings settings;
	private final CradleOperators ops;
	private final ExecutorService composingService;
	private final BookAndPageChecker bpc;
	private final SelectQueryExecutor selectExecutor;
	private final Function<BoundStatementBuilder, BoundStatementBuilder> writeAttrs;
	private final Function<BoundStatementBuilder, BoundStatementBuilder> readAttrs;
	
	public WorkerSupplies(CassandraStorageSettings settings, CradleOperators ops,
			ExecutorService composingService, BookAndPageChecker bpc,
			SelectQueryExecutor selectExecutor,
			Function<BoundStatementBuilder, BoundStatementBuilder> writeAttrs,
			Function<BoundStatementBuilder, BoundStatementBuilder> readAttrs)
	{
		this.settings = settings;
		this.ops = ops;
		this.composingService = composingService;
		this.bpc = bpc;
		this.selectExecutor = selectExecutor;
		this.writeAttrs = writeAttrs;
		this.readAttrs = readAttrs;
	}

	public CassandraStorageSettings getSettings()
	{
		return settings;
	}

	public CradleOperators getOps()
	{
		return ops;
	}

	public ExecutorService getComposingService()
	{
		return composingService;
	}

	public BookAndPageChecker getBpc()
	{
		return bpc;
	}

	public SelectQueryExecutor getSelectExecutor()
	{
		return selectExecutor;
	}

	public Function<BoundStatementBuilder, BoundStatementBuilder> getWriteAttrs()
	{
		return writeAttrs;
	}

	public Function<BoundStatementBuilder, BoundStatementBuilder> getReadAttrs()
	{
		return readAttrs;
	}
}

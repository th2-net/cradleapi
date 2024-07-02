/*
 * Copyright 2021-2024 Exactpro (Exactpro Systems Limited)
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

import com.datastax.oss.driver.api.core.cql.BatchStatementBuilder;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.exactpro.cradle.BookCache;
import com.exactpro.cradle.cassandra.CassandraStorageSettings;
import com.exactpro.cradle.cassandra.dao.CassandraOperators;
import com.exactpro.cradle.cassandra.retries.SelectQueryExecutor;

import java.util.concurrent.ExecutorService;
import java.util.function.Function;

public class WorkerSupplies {
    private final CassandraStorageSettings settings;
    private final CassandraOperators operators;
    private final ExecutorService composingService;
	private final BookCache bookCache;
    private final SelectQueryExecutor selectExecutor;
    private final Function<BoundStatementBuilder, BoundStatementBuilder> writeAttrs;
    private final Function<BoundStatementBuilder, BoundStatementBuilder> readAttrs;
    private final Function<BatchStatementBuilder, BatchStatementBuilder> batchWriteAttrs;
    public WorkerSupplies(CassandraStorageSettings settings, CassandraOperators operators,
                          ExecutorService composingService, BookCache BookCache,
                          SelectQueryExecutor selectExecutor,
                          Function<BoundStatementBuilder, BoundStatementBuilder> writeAttrs,
                          Function<BoundStatementBuilder, BoundStatementBuilder> readAttrs,
                          Function<BatchStatementBuilder, BatchStatementBuilder> batchWriteAttrs
    ) {
        this.settings = settings;
        this.operators = operators;
        this.composingService = composingService;
		this.bookCache = BookCache;
        this.selectExecutor = selectExecutor;
        this.writeAttrs = writeAttrs;
        this.readAttrs = readAttrs;
        this.batchWriteAttrs = batchWriteAttrs;
    }

    public CassandraStorageSettings getSettings() {
        return settings;
    }

    public CassandraOperators getOperators() {
        return operators;
    }

    public ExecutorService getComposingService() {
        return composingService;
    }

	public BookCache getBookCache()	{
		return bookCache;
	}

    public SelectQueryExecutor getSelectExecutor() {
        return selectExecutor;
    }

    public Function<BoundStatementBuilder, BoundStatementBuilder> getWriteAttrs() {
        return writeAttrs;
    }

    public Function<BoundStatementBuilder, BoundStatementBuilder> getReadAttrs() {
        return readAttrs;
    }

    public Function<BatchStatementBuilder, BatchStatementBuilder> getBatchWriteAttrs() {
        return batchWriteAttrs;
    }
}
/*
 * Copyright 2023-2024 Exactpro (Exactpro Systems Limited)
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

import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.exactpro.cradle.BookCache;
import com.exactpro.cradle.BookId;
import com.exactpro.cradle.cassandra.dao.CassandraOperators;
import com.exactpro.cradle.cassandra.dao.testevents.converters.PageScopeEntityConverter;
import com.exactpro.cradle.cassandra.iterators.PagedIterator;
import com.exactpro.cradle.cassandra.resultset.PagesInIntervalIteratorProvider;
import com.exactpro.cradle.cassandra.retries.SelectQueryExecutor;
import com.exactpro.cradle.counters.Interval;
import com.exactpro.cradle.iterators.ConvertingIterator;
import com.exactpro.cradle.iterators.UniqueIterator;
import com.exactpro.cradle.utils.CradleStorageException;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

public class PageScopesIteratorProvider extends PagesInIntervalIteratorProvider<String> {

    private final Set<String> registry = new HashSet<>();

    public PageScopesIteratorProvider(String requestInfo,
                                      CassandraOperators operators,
                                      BookId bookId,
                                      BookCache bookCache,
                                      Interval interval,
                                      ExecutorService composingService,
                                      SelectQueryExecutor selectQueryExecutor,
                                      Function<BoundStatementBuilder, BoundStatementBuilder> readAttrs) throws CradleStorageException {
        super(requestInfo, operators, bookId, bookCache, interval, composingService, selectQueryExecutor, readAttrs);
    }

    @Override
    public CompletableFuture<Iterator<String>> nextIterator() {
        // All pages have been processed, there can't be next iterator
        if (pages.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        PageScopesOperator pageScopesOperator = operators.getPageScopesOperator();
        PageScopeEntityConverter converter = operators.getPageScopeEntityConverter();

        return pageScopesOperator.getAsync(bookId.getName(), pages.remove(), readAttrs).thenApplyAsync(rs -> {
            PagedIterator<PageScopeEntity> pagedIterator = new PagedIterator<>(rs,
                    selectQueryExecutor,
                    converter::getEntity,
                    getRequestInfo());
            ConvertingIterator<PageScopeEntity, String> convertingIterator = new ConvertingIterator<>(
                    pagedIterator,
                    PageScopeEntity::getScope);

            return new UniqueIterator<>(convertingIterator, registry);
        }, composingService);
    }
}

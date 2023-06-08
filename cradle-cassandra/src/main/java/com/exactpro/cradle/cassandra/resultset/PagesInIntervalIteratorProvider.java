/*
 * Copyright 2023 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.cradle.cassandra.resultset;

import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.exactpro.cradle.BookCache;
import com.exactpro.cradle.BookId;
import com.exactpro.cradle.PageInfo;
import com.exactpro.cradle.cassandra.dao.CassandraOperators;
import com.exactpro.cradle.cassandra.retries.SelectQueryExecutor;
import com.exactpro.cradle.counters.Interval;
import com.exactpro.cradle.utils.CradleStorageException;

import java.time.Instant;
import java.util.LinkedList;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Iterator provider which provides different iterators for each
 * page that is the given interval of time and belongs the given book
 *
 * @param <T> type of the iterated object
 */
public abstract class PagesInIntervalIteratorProvider<T> extends IteratorProvider<T> {

    protected final CassandraOperators operators;
    protected final ExecutorService composingService;
    protected final SelectQueryExecutor selectQueryExecutor;
    protected final BookId bookId;
    protected final Queue<String> pages;
    protected final Function<BoundStatementBuilder, BoundStatementBuilder> readAttrs;

    public PagesInIntervalIteratorProvider(String requestInfo,
                                           CassandraOperators operators,
                                           BookId bookId,
                                           BookCache bookCache,
                                           Interval interval,
                                           ExecutorService composingService,
                                           SelectQueryExecutor selectQueryExecutor,
                                           Function<BoundStatementBuilder, BoundStatementBuilder> readAttrs) throws CradleStorageException {
        super(requestInfo);
        this.operators = operators;
        this.bookId = bookId;
        this.pages = getPagesInInterval(bookId, bookCache, interval);
        this.composingService = composingService;
        this.selectQueryExecutor = selectQueryExecutor;
        this.readAttrs = readAttrs;
    }

    private Queue<String> getPagesInInterval(BookId bookId, BookCache bookCache, Interval interval) throws CradleStorageException {
        Instant start = interval.getStart();
        Instant end = interval.getEnd();

        return bookCache.loadPageInfo(bookId, false)
                .stream()
                .filter(page -> checkInterval(page, start, end))
                .map(page -> page.getId().getName())
                .collect(Collectors.toCollection(LinkedList::new));
    }

    private boolean checkInterval(PageInfo page, Instant start, Instant end) {
        var pageStart = page.getStarted();
        Objects.requireNonNull(pageStart, String.format("Page \"%s\" has null start time", page.getId().getName()));
        var pageEnd = page.getEnded();
        if (pageEnd == null) {
            return pageStart.isAfter(start) && pageStart.isBefore(end);
        } else {
            return !pageEnd.isBefore(start) && !pageStart.isAfter(end);
        }
    }
}

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
import com.exactpro.cradle.BookInfo;
import com.exactpro.cradle.PageInfo;
import com.exactpro.cradle.cassandra.counters.FrameInterval;
import com.exactpro.cradle.cassandra.dao.CassandraOperators;
import com.exactpro.cradle.cassandra.dao.statistics.scopes.ScopeStatisticsEntity;
import com.exactpro.cradle.cassandra.dao.statistics.scopes.ScopeStatisticsEntityConverter;
import com.exactpro.cradle.cassandra.dao.statistics.scopes.ScopeStatisticsOperator;
import com.exactpro.cradle.cassandra.iterators.DuplicateSkippingIterator;
import com.exactpro.cradle.cassandra.retries.SelectQueryExecutor;
import com.exactpro.cradle.cassandra.utils.FilterUtils;
import com.exactpro.cradle.filters.FilterForGreater;

import java.time.Instant;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/**
 * Iterator provider for sessions which provides different iterators for
 * frameIntervals and pages
 */
public class ScopeStatisticsIteratorProvider extends IteratorProvider<String> {


    private final CassandraOperators operators;
    private final BookInfo bookInfo;
    private final ExecutorService composingService;
    private final SelectQueryExecutor selectQueryExecutor;
    private final Function<BoundStatementBuilder, BoundStatementBuilder> readAttrs;
    private final List<FrameInterval> frameIntervals;
    private Integer frameIndex;
    /*
        This set will be used during creation of all next iterators
        to guarantee that unique elements will be returned
        across all iterators
     */
    private final Set<Long> registry;
    private PageInfo curPage;

    public ScopeStatisticsIteratorProvider(String requestInfo,
                                           CassandraOperators operators,
                                           BookInfo bookInfo,
                                           ExecutorService composingService,
                                           SelectQueryExecutor selectQueryExecutor,
                                           Function<BoundStatementBuilder, BoundStatementBuilder> readAttrs,
                                           List<FrameInterval> frameIntervals) {
        super(requestInfo);
        this.operators = operators;
        this.bookInfo = bookInfo;
        this.composingService = composingService;
        this.selectQueryExecutor = selectQueryExecutor;
        this.readAttrs = readAttrs;
        this.frameIntervals = frameIntervals;

        this.registry = new HashSet<>();
        /*
            Since intervals are created in strictly increasing, non-overlapping order
            the first page is set in regards to first interval
         */
        this.frameIndex = 0;
        this.curPage = FilterUtils.findFirstPage(null, FilterForGreater.forGreater(frameIntervals.get(frameIndex).getInterval().getStart()), bookInfo);
    }

    @Override
    public CompletableFuture<Iterator<String>> nextIterator() {
        // All intervals have been processed, there can't be next iterator
        if (frameIndex == frameIntervals.size()) {
            return CompletableFuture.completedFuture(null);
        }

        FrameInterval frameInterval = frameIntervals.get(frameIndex);

        Instant actualStart = frameInterval.getInterval().getStart();
        Instant actualEnd = frameInterval.getInterval().getEnd();

        ScopeStatisticsOperator scopeStatisticsOperator = operators.getScopeStatisticsOperator();
        ScopeStatisticsEntityConverter converter = operators.getScopeStatisticsEntityConverter();

        return scopeStatisticsOperator.getStatistics(
                curPage.getId().getBookId().getName(),
                curPage.getId().getName(),
                frameInterval.getFrameType().getValue(),
                actualStart,
                actualEnd,
                readAttrs).thenApplyAsync(rs -> {
                                /*
                                    At this point we need to either update page
                                    or move to new interval
                                 */
            if (curPage.getEnded() != null && curPage.getEnded().isBefore(frameInterval.getInterval().getEnd())) {
                // Page finishes sooner than this interval
                curPage = bookInfo.getNextPage(curPage.getStarted());
            } else {
                // Interval finishes sooner than page
                frameIndex++;
            }

            return new DuplicateSkippingIterator<>(rs,
                    selectQueryExecutor,
                    -1,
                    new AtomicInteger(0),
                    ScopeStatisticsEntity::getScope,
                    converter::getEntity,
                    (str) -> Long.valueOf(str.hashCode()),
                    registry,
                    getRequestInfo());
        }, composingService);
    }
}

/*
 * Copyright 2022-2024 Exactpro (Exactpro Systems Limited)
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
package com.exactpro.cradle.cassandra.dao.statistics;

import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.exactpro.cradle.BookInfo;
import com.exactpro.cradle.EntityType;
import com.exactpro.cradle.FrameType;
import com.exactpro.cradle.PageInfo;
import com.exactpro.cradle.cassandra.counters.FrameInterval;
import com.exactpro.cradle.cassandra.dao.CassandraOperators;
import com.exactpro.cradle.cassandra.iterators.PagedIterator;
import com.exactpro.cradle.cassandra.resultset.IteratorProvider;
import com.exactpro.cradle.cassandra.retries.SelectQueryExecutor;
import com.exactpro.cradle.cassandra.utils.ThreadSafeProvider;
import com.exactpro.cradle.counters.CounterSample;
import com.exactpro.cradle.iterators.ConvertingIterator;

import java.time.Instant;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

import static com.exactpro.cradle.Order.DIRECT;

public class EntityStatisticsIteratorProvider extends IteratorProvider<CounterSample> {
    private final CassandraOperators operators;
    private final ExecutorService composingService;
    private final SelectQueryExecutor selectQueryExecutor;
    private final EntityType entityType;
    private final FrameType frameType;
    private final FrameInterval frameInterval;
    private final Function<BoundStatementBuilder, BoundStatementBuilder> readAttrs;
    private final ThreadSafeProvider<PageInfo> pageProvider;
    public EntityStatisticsIteratorProvider(String requestInfo, CassandraOperators operators, BookInfo book,
                                            ExecutorService composingService, SelectQueryExecutor selectQueryExecutor,
                                            EntityType entityType,
                                            FrameType frameType,
                                            FrameInterval frameInterval,
                                            Function<BoundStatementBuilder, BoundStatementBuilder> readAttrs) {
        super(requestInfo);
        this.operators = operators;
        this.composingService = composingService;
        this.selectQueryExecutor = selectQueryExecutor;
        this.entityType = entityType;
        this.frameType = frameType;
        this.frameInterval = frameInterval;
        this.readAttrs = readAttrs;
        this.pageProvider = new ThreadSafeProvider<>(
                book.getPages(
                        frameInterval.getInterval().getStart(),
                        frameInterval.getInterval().getEnd(),
                        DIRECT
                )
        );
    }

    @Override
    public CompletableFuture<Iterator<CounterSample>> nextIterator() {
        PageInfo pageInfo = pageProvider.next();
        if(pageInfo == null){
            return CompletableFuture.completedFuture(null);
        }

        Instant actualStart = frameInterval.getFrameType().getFrameStart(frameInterval.getInterval().getStart());
        Instant actualEnd = frameInterval.getFrameType().getFrameEnd(frameInterval.getInterval().getEnd());

        EntityStatisticsOperator entityStatsOperator = operators.getEntityStatisticsOperator();
        EntityStatisticsEntityConverter entityStatsConverter = operators.getEntityStatisticsEntityConverter();

        return entityStatsOperator.getStatistics(
                        pageInfo.getBookName(),
                        pageInfo.getName(),
						entityType.getValue(),
						frameType.getValue(),
						actualStart,
						actualEnd,
						readAttrs)
				.thenApplyAsync(rs -> {
                            PagedIterator<EntityStatisticsEntity> pagedIterator =  new PagedIterator<>(
                                    rs,
                                    selectQueryExecutor,
                                    entityStatsConverter::getEntity,
                                    getRequestInfo());

                            return new ConvertingIterator<>(
                                    pagedIterator,
                                    EntityStatisticsEntity::toCounterSample);
                }, composingService);
    }
}

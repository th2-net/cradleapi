package com.exactpro.cradle.cassandra.dao;

import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.exactpro.cradle.*;
import com.exactpro.cradle.cassandra.counters.CounterSamples;
import com.exactpro.cradle.cassandra.counters.FrameInterval;
import com.exactpro.cradle.cassandra.iterators.ConvertingPagedIterator;
import com.exactpro.cradle.cassandra.resultset.CassandraCradleResultSet;
import com.exactpro.cradle.cassandra.resultset.IteratorProvider;
import com.exactpro.cradle.cassandra.retries.SelectQueryExecutor;
import com.exactpro.cradle.cassandra.utils.FilterUtils;
import com.exactpro.cradle.counters.Counter;
import com.exactpro.cradle.counters.CounterSample;
import com.exactpro.cradle.filters.FilterForGreater;

import java.time.Instant;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

public class EntityStatisticsIteratorProvider extends IteratorProvider<CounterSample> {
    private BookOperators ops;
    private BookInfo book;
    private ExecutorService composingService;
    private SelectQueryExecutor selectQueryExecutor;
    private EntityType entityType;
    private FrameType frameType;
    private FrameInterval frameInterval;
    private Function<BoundStatementBuilder, BoundStatementBuilder> readAttrs;
    private PageInfo currentPage;
    public EntityStatisticsIteratorProvider(String requestInfo, BookOperators ops, BookInfo book,
                                            ExecutorService composingService, SelectQueryExecutor selectQueryExecutor,
                                            EntityType entityType,
                                            FrameType frameType,
                                            FrameInterval frameInterval,
                                            Function<BoundStatementBuilder, BoundStatementBuilder> readAttrs) {
        super(requestInfo);
        this.ops = ops;
        this.book = book;
        this.composingService = composingService;
        this.selectQueryExecutor = selectQueryExecutor;
        this.entityType = entityType;
        this.frameType = frameType;
        this.frameInterval = frameInterval;
        this.readAttrs = readAttrs;
        this.currentPage = FilterUtils.findFirstPage(null, FilterForGreater.forGreater(frameInterval.getInterval().getStart()),book);
    }

    @Override
    public CompletableFuture<Iterator<CounterSample>> nextIterator() {
        if(currentPage == null || frameInterval.getInterval().getEnd().isBefore(currentPage.getStarted()) ){
            return CompletableFuture.completedFuture(null);
        }

        Instant actualStart = frameInterval.getFrameType().getFrameStart(frameInterval.getInterval().getStart());
        Instant actualEnd = frameInterval.getFrameType().getFrameEnd(frameInterval.getInterval().getEnd());

        EntityStatisticsOperator entityStatsOperator = ops.getEntityStatisticsOperator();
        EntityStatisticsEntityConverter entityStatsConverter = ops.getEntityStatisticsEntityConverter();

        return entityStatsOperator.getStatistics(
						entityType.getValue(),
						frameType.getValue(),
						actualStart,
						actualEnd,
						readAttrs)
				.thenApplyAsync(rs -> {
                            currentPage = book.getNextPage(currentPage.getStarted());
                            return new ConvertingPagedIterator<>(rs,
                                    selectQueryExecutor,
                                    -1,
                                    new AtomicInteger(0),
                                    EntityStatisticsEntity::toCounterSample,
                                    entityStatsConverter::getEntity, this.getRequestInfo());
                });
    }
}

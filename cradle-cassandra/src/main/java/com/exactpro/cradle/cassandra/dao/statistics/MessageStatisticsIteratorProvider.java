package com.exactpro.cradle.cassandra.dao.statistics;

import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.exactpro.cradle.BookInfo;
import com.exactpro.cradle.Direction;
import com.exactpro.cradle.PageInfo;
import com.exactpro.cradle.cassandra.counters.FrameInterval;
import com.exactpro.cradle.cassandra.dao.CassandraOperators;
import com.exactpro.cradle.cassandra.iterators.PagedIterator;
import com.exactpro.cradle.cassandra.resultset.IteratorProvider;
import com.exactpro.cradle.cassandra.retries.SelectQueryExecutor;
import com.exactpro.cradle.cassandra.utils.FilterUtils;
import com.exactpro.cradle.counters.CounterSample;
import com.exactpro.cradle.filters.FilterForGreater;
import com.exactpro.cradle.iterators.ConvertingIterator;

import java.time.Instant;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

public class MessageStatisticsIteratorProvider extends IteratorProvider<CounterSample> {
    private CassandraOperators operators;
    private BookInfo book;
    private ExecutorService composingService;
    private SelectQueryExecutor selectQueryExecutor;
    private String sessionAlias;
    private Direction direction;
    private FrameInterval frameInterval;
    private Function<BoundStatementBuilder, BoundStatementBuilder> readAttrs;
    private PageInfo currentPage;


    public MessageStatisticsIteratorProvider(String requestInfo, CassandraOperators operators, BookInfo book,
                                             ExecutorService composingService, SelectQueryExecutor selectQueryExecutor,
                                             String sessionAlias, Direction direction, FrameInterval frameInterval,
                                             Function<BoundStatementBuilder, BoundStatementBuilder> readAttrs) {
        super(requestInfo);

        this.operators = operators;
        this.book = book;
        this.composingService = composingService;
        this.selectQueryExecutor = selectQueryExecutor;
        this.sessionAlias = sessionAlias;
        this.direction = direction;
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

        MessageStatisticsOperator messageStatsOperator = operators.getMessageStatisticsOperator();
        MessageStatisticsEntityConverter messageStatsConverter = operators.getMessageStatisticsEntityConverter();

        return messageStatsOperator.getStatistics(
                        currentPage.getId().getBookId().getName(),
                        currentPage.getId().getName(),
                        sessionAlias,
                        direction.getLabel(),
                        frameInterval.getFrameType().getValue(),
                        actualStart,
                        actualEnd,
                        readAttrs)
                .thenApplyAsync(rs -> {
                            currentPage = book.getNextPage(currentPage.getStarted());

                            PagedIterator<MessageStatisticsEntity> pagedIterator = new PagedIterator<>(
                                    rs,
                                    selectQueryExecutor,
                                    messageStatsConverter::getEntity,
                                    getRequestInfo());

                            return new ConvertingIterator<>(
                                    pagedIterator,
                                    MessageStatisticsEntity::toCounterSample);
                        }, composingService);
    }
}
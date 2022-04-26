package com.exactpro.cradle.cassandra.resultset;

import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.exactpro.cradle.BookInfo;
import com.exactpro.cradle.PageInfo;
import com.exactpro.cradle.cassandra.counters.SessionRecordFrameInterval;
import com.exactpro.cradle.cassandra.dao.BookOperators;
import com.exactpro.cradle.cassandra.dao.SessionStatisticsEntity;
import com.exactpro.cradle.cassandra.dao.SessionStatisticsEntityConverter;
import com.exactpro.cradle.cassandra.dao.SessionStatisticsOperator;
import com.exactpro.cradle.cassandra.iterators.DuplicateSkippingIterator;
import com.exactpro.cradle.cassandra.retries.SelectQueryExecutor;
import com.exactpro.cradle.cassandra.utils.FilterUtils;
import com.exactpro.cradle.filters.FilterForGreater;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

public class SessionsStatisticsIteratorProvider extends IteratorProvider<String>{


    private final BookOperators bookOperators;
    private final BookInfo bookInfo;
    private final ExecutorService composingService;
    private final SelectQueryExecutor selectQueryExecutor;
    private final Function<BoundStatementBuilder, BoundStatementBuilder> readAttrs;
    private final SessionRecordFrameInterval sessionRecordFrameInterval;
    private final Set<Long> set;
    private PageInfo curPage;

    public SessionsStatisticsIteratorProvider (String requestInfo,
                                               BookOperators bookOperators,
                                               BookInfo bookInfo,
                                               ExecutorService composingService,
                                               SelectQueryExecutor selectQueryExecutor,
                                               Function<BoundStatementBuilder, BoundStatementBuilder> readAttrs,
                                               SessionRecordFrameInterval sessionRecordFrameInterval) {
        super(requestInfo);
        this.bookOperators = bookOperators;
        this.bookInfo = bookInfo;
        this.composingService = composingService;
        this.selectQueryExecutor = selectQueryExecutor;
        this.readAttrs = readAttrs;
        this.sessionRecordFrameInterval = sessionRecordFrameInterval;
        this.set = new HashSet<>();

        this.curPage = FilterUtils.findFirstPage(null, FilterForGreater.forGreater(sessionRecordFrameInterval.getInterval().getStart()), bookInfo);
    }

    @Override
    public CompletableFuture<Iterator<String>> nextIterator() {
        if (curPage == null || curPage.getStarted().isAfter(sessionRecordFrameInterval.getInterval().getEnd())) {
            return CompletableFuture.completedFuture(null);
        }

        SessionStatisticsOperator sessionStatisticsOperator = bookOperators.getSessionStatisticsOperator();
        SessionStatisticsEntityConverter converter = bookOperators.getSessionStatisticsEntityConverter();

        return sessionStatisticsOperator.getStatistics(curPage.getId().getName(),
                        sessionRecordFrameInterval.getSessionRecordType().getValue(),
                        sessionRecordFrameInterval.getFrameType().getValue(),
                        sessionRecordFrameInterval.getInterval().getStart(),
                        sessionRecordFrameInterval.getInterval().getEnd(),
                        readAttrs).thenApplyAsync(rs -> {
                                curPage = bookInfo.getNextPage(curPage.getStarted());

                                return new DuplicateSkippingIterator<>(rs,
                                        selectQueryExecutor,
                                        -1,
                                        new AtomicInteger(0),
                                        SessionStatisticsEntity::getSession,
                                        converter::getEntity,
                                        (obj) -> Long.valueOf(obj.hashCode()),
                                        set,
                                        getRequestInfo());
                                }, composingService);
        }
}

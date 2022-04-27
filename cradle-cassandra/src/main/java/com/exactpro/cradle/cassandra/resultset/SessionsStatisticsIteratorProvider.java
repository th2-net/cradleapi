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

import java.time.Instant;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
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
    private final List<SessionRecordFrameInterval> sessionRecordFrameIntervals;
    private final Set<Long> set;
    private PageInfo curPage;

    public SessionsStatisticsIteratorProvider (String requestInfo,
                                               BookOperators bookOperators,
                                               BookInfo bookInfo,
                                               ExecutorService composingService,
                                               SelectQueryExecutor selectQueryExecutor,
                                               Function<BoundStatementBuilder, BoundStatementBuilder> readAttrs,
                                               List<SessionRecordFrameInterval> sessionRecordFrameIntervals) {
        super(requestInfo);
        this.bookOperators = bookOperators;
        this.bookInfo = bookInfo;
        this.composingService = composingService;
        this.selectQueryExecutor = selectQueryExecutor;
        this.readAttrs = readAttrs;
        this.sessionRecordFrameIntervals = sessionRecordFrameIntervals;
        this.set = new HashSet<>();

        this.curPage = FilterUtils.findFirstPage(null, FilterForGreater.forGreater(sessionRecordFrameIntervals.get(0).getInterval().getStart()), bookInfo);
    }

    @Override
    public CompletableFuture<Iterator<String>> nextIterator() {
        if (sessionRecordFrameIntervals.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        SessionRecordFrameInterval sessionRecordFrameInterval = sessionRecordFrameIntervals.get(0);

        Instant actualStart = sessionRecordFrameInterval.getFrameType().getFrameStart(sessionRecordFrameInterval.getInterval().getStart());
        Instant actualEnd = sessionRecordFrameInterval.getFrameType().getFrameEnd(sessionRecordFrameInterval.getInterval().getEnd());

        SessionStatisticsOperator sessionStatisticsOperator = bookOperators.getSessionStatisticsOperator();
        SessionStatisticsEntityConverter converter = bookOperators.getSessionStatisticsEntityConverter();

        return sessionStatisticsOperator.getStatistics(curPage.getId().getName(),
                        sessionRecordFrameInterval.getSessionRecordType().getValue(),
                        sessionRecordFrameInterval.getFrameType().getValue(),
                        actualStart,
                        actualEnd,
                        readAttrs).thenApplyAsync(rs -> {
                                /*
                                    At this point we need to either update page
                                    or move to new interval
                                 */
                                if (curPage.getEnded() != null && curPage.getEnded().isBefore(sessionRecordFrameInterval.getInterval().getEnd())) {
                                    // Page finishes sooner than this interval
                                    curPage = bookInfo.getNextPage(curPage.getStarted());
                                } else {
                                    // Interval finishes sooner than page
                                    sessionRecordFrameIntervals.remove(0);
                                }

                                return new DuplicateSkippingIterator<>(rs,
                                        selectQueryExecutor,
                                        -1,
                                        new AtomicInteger(0),
                                        SessionStatisticsEntity::getSession,
                                        converter::getEntity,
                                        (str) -> Long.valueOf(str.hashCode()),
                                        set,
                                        getRequestInfo());
                                }, composingService);
        }
}

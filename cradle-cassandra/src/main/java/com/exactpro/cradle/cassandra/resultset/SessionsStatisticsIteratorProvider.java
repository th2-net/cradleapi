package com.exactpro.cradle.cassandra.resultset;

import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.exactpro.cradle.BookInfo;
import com.exactpro.cradle.PageInfo;
import com.exactpro.cradle.SessionRecordType;
import com.exactpro.cradle.cassandra.counters.FrameInterval;
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

/**
 * Iterator provider for sessions which provides different iterators for
 * frameIntervals and pages
 */
public class SessionsStatisticsIteratorProvider extends IteratorProvider<String>{


    private final BookOperators bookOperators;
    private final BookInfo bookInfo;
    private final ExecutorService composingService;
    private final SelectQueryExecutor selectQueryExecutor;
    private final Function<BoundStatementBuilder, BoundStatementBuilder> readAttrs;
    private final List<FrameInterval> frameIntervals;
    private final SessionRecordType recordType;
    private Integer frameIndex;
    /*
        This set will be used during creation of all next iterators
        to guarantee that unique elements will be returned
        across all iterators
     */
    private final Set<Long> registry;
    private PageInfo curPage;

    public SessionsStatisticsIteratorProvider (String requestInfo,
                                               BookOperators bookOperators,
                                               BookInfo bookInfo,
                                               ExecutorService composingService,
                                               SelectQueryExecutor selectQueryExecutor,
                                               Function<BoundStatementBuilder, BoundStatementBuilder> readAttrs,
                                               List<FrameInterval> frameIntervals,
                                               SessionRecordType recordType) {
        super(requestInfo);
        this.bookOperators = bookOperators;
        this.bookInfo = bookInfo;
        this.composingService = composingService;
        this.selectQueryExecutor = selectQueryExecutor;
        this.readAttrs = readAttrs;
        this.frameIntervals = frameIntervals;
        this.recordType = recordType;

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

        Instant actualStart = frameInterval.getFrameType().getFrameStart(frameInterval.getInterval().getStart());
        Instant actualEnd = frameInterval.getFrameType().getFrameEnd(frameInterval.getInterval().getEnd());

        SessionStatisticsOperator sessionStatisticsOperator = bookOperators.getSessionStatisticsOperator();
        SessionStatisticsEntityConverter converter = bookOperators.getSessionStatisticsEntityConverter();

        return sessionStatisticsOperator.getStatistics(curPage.getId().getName(),
                        recordType.getValue(),
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
                                    frameIndex ++;
                                }

                                return new DuplicateSkippingIterator<>(rs,
                                        selectQueryExecutor,
                                        -1,
                                        new AtomicInteger(0),
                                        SessionStatisticsEntity::getSession,
                                        converter::getEntity,
                                        (str) -> Long.valueOf(str.hashCode()),
                                        registry,
                                        getRequestInfo());
                                }, composingService);
        }
}

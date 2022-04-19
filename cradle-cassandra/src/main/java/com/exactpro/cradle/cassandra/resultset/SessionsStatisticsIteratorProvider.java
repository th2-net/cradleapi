package com.exactpro.cradle.cassandra.resultset;

import com.exactpro.cradle.cassandra.counters.FrameInterval;
import com.exactpro.cradle.cassandra.dao.SessionStatisticsEntity;
import com.exactpro.cradle.cassandra.iterators.DuplicateSkippingIterator;
import com.exactpro.cradle.cassandra.retries.SelectQueryExecutor;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class SessionsStatisticsIteratorProvider extends IteratorProvider<SessionStatisticsEntity>{

    private final List<FrameInterval> frameIntervals;
    private DuplicateSkippingIterator<Object, Object> recentIterator;

    //TODO: pass session statistics operator, converter, mapper
    public SessionsStatisticsIteratorProvider (String requestInfo,
//                                                sessionStatisticsOperator,
                                               SelectQueryExecutor selectQueryExecutor,
//                                                converter,
//                                                mapper,
                                               List<FrameInterval> frameIntervals) {
        super(requestInfo);
        this.frameIntervals = frameIntervals;
        this.recentIterator = null;
    }

    @Override
    public CompletableFuture<Iterator<SessionStatisticsEntity>> nextIterator() {
        if (frameIntervals.isEmpty()) {
            return null;
        }

        FrameInterval frameInterval = frameIntervals.remove(0);

        //TODO: create skipping iterator from frame interval;
        return null;
    }
}

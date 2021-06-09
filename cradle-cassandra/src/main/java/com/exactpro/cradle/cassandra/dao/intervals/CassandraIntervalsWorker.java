package com.exactpro.cradle.cassandra.dao.intervals;

import com.datastax.oss.driver.api.core.MappedAsyncPagingIterable;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.exactpro.cradle.cassandra.CassandraSemaphore;
import com.exactpro.cradle.cassandra.dao.AsyncOperator;
import com.exactpro.cradle.cassandra.iterators.IntervalsIteratorAdapter;
import com.exactpro.cradle.intervals.Interval;
import com.exactpro.cradle.intervals.IntervalsWorker;
import com.exactpro.cradle.intervals.RecoveryState;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Function;

import static com.exactpro.cradle.cassandra.CassandraCradleStorage.TIMEZONE_OFFSET;

public class CassandraIntervalsWorker implements IntervalsWorker
{
    private final CassandraSemaphore semaphore;
    private final UUID instanceUuid;
    private final Function<BoundStatementBuilder, BoundStatementBuilder> writeAttrs, readAttrs;
    private final IntervalOperator intervalOperator;

    public CassandraIntervalsWorker(CassandraSemaphore semaphore, UUID instanceUuid, Function<BoundStatementBuilder,
            BoundStatementBuilder> writeAttrs, Function<BoundStatementBuilder,
            BoundStatementBuilder> readAttrs, IntervalOperator intervalOperator)
    {
        this.semaphore = semaphore;
        this.instanceUuid = instanceUuid;
        this.writeAttrs = writeAttrs;
        this.readAttrs = readAttrs;
        this.intervalOperator = intervalOperator;
    }

    @Override
    public void storeInterval(Interval interval) throws IOException
    {
        try
        {
            storeIntervalAsync(interval).get();
        }
        catch (Exception e)
        {
            throw new IOException("Error while storing interval "+interval.getId(), e);
        }
    }

    @Override
    public CompletableFuture<Void> storeIntervalAsync(Interval interval)
    {
        CompletableFuture<IntervalEntity> future = new AsyncOperator<IntervalEntity>(semaphore)
                .getFuture(() -> {
                    IntervalEntity intervalEntity = new IntervalEntity(interval, instanceUuid);
                    return intervalOperator.writeInterval(intervalEntity, writeAttrs);
                });

        return future.thenAccept(r -> {});
    }

    @Override
    public Iterable<Interval> getIntervals(Instant from, Instant to, String crawlerName, String crawlerVersion) throws IOException
    {
        try
        {
            return getIntervalsAsync(from, to, crawlerName, crawlerVersion).get();
        }
        catch (Exception e)
        {
            throw new IOException("Error while getting healing intervals from " + from, e);
        }
    }

    @Override
    public CompletableFuture<Iterable<Interval>> getIntervalsAsync(Instant from, Instant to, String crawlerName, String crawlerVersion)
    {
        LocalDateTime fromDateTime = LocalDateTime.ofInstant(from, TIMEZONE_OFFSET),
                toDateTime = LocalDateTime.ofInstant(to, TIMEZONE_OFFSET);

        LocalTime fromTime = fromDateTime.toLocalTime(),
                toTime = toDateTime.toLocalTime();

        LocalDate date = fromDateTime.toLocalDate();

        CompletableFuture<MappedAsyncPagingIterable<IntervalEntity>> future =
                new AsyncOperator<MappedAsyncPagingIterable<IntervalEntity>>(semaphore)
                        .getFuture(() -> intervalOperator
                                .getIntervals(instanceUuid, date, fromTime, toTime, crawlerName, crawlerVersion, readAttrs));

        return future.thenApply(entities -> {
            try
            {
                return new IntervalsIteratorAdapter(entities);
            }
            catch (Exception error)
            {
                throw new CompletionException("Could not get healing intervals from "+from, error);
            }
        });
    }

    @Override
    public boolean setIntervalLastUpdateTimeAndDate(Interval interval, Instant newLastUpdateTime) throws IOException
    {
        try
        {
            return setIntervalLastUpdateTimeAndDateAsync(interval, newLastUpdateTime).get();
        }
		catch (Exception e)
        {
            throw new IOException("Error while occupying interval "+interval.getId(), e);
        }
    }

    @Override
    public CompletableFuture<Boolean> setIntervalLastUpdateTimeAndDateAsync(Interval interval, Instant newLastUpdateTime)
    {
        LocalDateTime dateTime = LocalDateTime.ofInstant(newLastUpdateTime, TIMEZONE_OFFSET);

        LocalTime time = dateTime.toLocalTime();
        LocalDate date = dateTime.toLocalDate();

        CompletableFuture<AsyncResultSet> future = new AsyncOperator<AsyncResultSet>(semaphore)
                .getFuture(() ->
                        intervalOperator.setIntervalLastUpdateTimeAndDate(instanceUuid, interval.getId(), interval.getDate(),
                                interval.getStartTime(), time, date, interval.getLastUpdateTime(), interval.getLastUpdateDate(),
                                interval.getCrawlerName(), interval.getCrawlerVersion(),
                                writeAttrs));
        return future.thenApply(AsyncResultSet::wasApplied);
    }

    @Override
    public void updateRecoveryState(Interval interval, RecoveryState recoveryState) throws IOException
    {
        try
        {
            updateRecoveryStateAsync(interval, recoveryState);
        }
        catch (Exception e)
        {
            throw new IOException("Error while updating recovery state of interval "+interval.getId(), e);
        }
    }

    @Override
    public CompletableFuture<Void> updateRecoveryStateAsync(Interval interval, RecoveryState recoveryState)
    {
        CompletableFuture<AsyncResultSet> future = new AsyncOperator<AsyncResultSet>(semaphore)
                .getFuture(() -> intervalOperator.updateRecoveryState(instanceUuid, interval.getDate(),
                        interval.getStartTime(), interval.getId(), interval.getCrawlerName(),
                        interval.getCrawlerVersion(), recoveryState.convertToJson()));

        return future.thenAccept(r -> {});
    }
}

/*
 * Copyright 2020-2021 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.cradle.cassandra.dao.intervals;

import com.datastax.oss.driver.api.core.MappedAsyncPagingIterable;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.shaded.guava.common.collect.Iterables;
import com.exactpro.cradle.cassandra.CassandraSemaphore;
import com.exactpro.cradle.cassandra.dao.AsyncOperator;
import com.exactpro.cradle.cassandra.iterators.IntervalsIteratorAdapter;
import com.exactpro.cradle.intervals.Interval;
import com.exactpro.cradle.intervals.IntervalsWorker;
import com.exactpro.cradle.utils.CradleStorageException;
import com.exactpro.cradle.utils.UpdateNotAppliedException;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
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
    public boolean storeInterval(Interval interval) throws IOException
    {
        try
        {
            return storeIntervalAsync(interval).get();
        }
        catch (Exception e)
        {
            throw new IOException("Error while storing interval from: "+interval.getStartTime()+", to: "
                    +interval.getEndTime()+", name: "+interval.getCrawlerName()+", version: "
                    +interval.getCrawlerVersion()+", type: "+interval.getCrawlerType(), e);
        }
    }

    @Override
    public CompletableFuture<Boolean> storeIntervalAsync(Interval interval)
    {
        CompletableFuture<AsyncResultSet> future = new AsyncOperator<AsyncResultSet>(semaphore)
                .getFuture(() -> {
                    IntervalEntity intervalEntity = new IntervalEntity(interval, instanceUuid);
                    return intervalOperator.writeInterval(intervalEntity, writeAttrs);
                });

        return future.thenApply(AsyncResultSet::wasApplied);
    }

    @Override
    public Iterable<Interval> getIntervalsPerDay(Instant from, Instant to, String crawlerName, String crawlerVersion, String crawlerType) throws IOException
    {
        try
        {
            return getIntervalsPerDayAsync(from, to, crawlerName, crawlerVersion, crawlerType).get();
        }
        catch (Exception e)
        {
            throw new IOException("Error while getting intervals from: "+from+", to: "+to+" by Crawler with " +
                    "name: "+crawlerName+", version: "+crawlerVersion+", type: "+crawlerType, e);
        }
    }

    @Override
    public CompletableFuture<Iterable<Interval>> getIntervalsPerDayAsync(Instant from, Instant to, String crawlerName,
                                                                         String crawlerVersion, String crawlerType) throws CradleStorageException
    {
        LocalDateTime fromDateTime = LocalDateTime.ofInstant(from, TIMEZONE_OFFSET),
                toDateTime = LocalDateTime.ofInstant(to, TIMEZONE_OFFSET);

        checkTimeBoundaries(fromDateTime, toDateTime, from, to);

        LocalTime fromTime = fromDateTime.toLocalTime(),
                toTime = toDateTime.toLocalTime();

        LocalDate date = fromDateTime.toLocalDate();

        CompletableFuture<MappedAsyncPagingIterable<IntervalEntity>> future =
                new AsyncOperator<MappedAsyncPagingIterable<IntervalEntity>>(semaphore)
                        .getFuture(() -> intervalOperator
                                .getIntervals(instanceUuid, date, fromTime, toTime, crawlerName, crawlerVersion, crawlerType, readAttrs));

        return future.thenApply(IntervalsIteratorAdapter::new);
    }

    private Iterable<Interval> getIntervalsPerDay(LocalDateTime from, LocalDateTime to, String crawlerName, String crawlerVersion, String crawlerType) throws IOException {
        try
        {
            return getIntervalsPerDayAsync(from, to, crawlerName, crawlerVersion, crawlerType).get();
        }
        catch (Exception e)
        {
            throw new IOException("Error while getting intervals from: "+from+", to: "+to+" by Crawler with " +
                    "name: "+crawlerName+", version: "+crawlerVersion+", type: "+crawlerType, e);
        }
    }

    private CompletableFuture<Iterable<Interval>> getIntervalsPerDayAsync(LocalDateTime from, LocalDateTime to, String crawlerName,
                                                                         String crawlerVersion, String crawlerType) throws CradleStorageException
    {
        checkTimeBoundaries(from, to);

        LocalTime fromTime = from.toLocalTime(),
                toTime = to.toLocalTime();

        LocalDate date = from.toLocalDate();

        CompletableFuture<MappedAsyncPagingIterable<IntervalEntity>> future =
                new AsyncOperator<MappedAsyncPagingIterable<IntervalEntity>>(semaphore)
                        .getFuture(() -> intervalOperator
                                .getIntervals(instanceUuid, date, fromTime, toTime, crawlerName, crawlerVersion, crawlerType, readAttrs));

        return future.thenApply(IntervalsIteratorAdapter::new);
    }

    @Override
    public Iterable<Interval> getIntervals(Instant from, Instant to, String crawlerName, String crawlerVersion, String crawlerType) throws IOException
    {
        LocalDateTime fromDateTime = LocalDateTime.ofInstant(from, TIMEZONE_OFFSET),
                toDateTime = LocalDateTime.ofInstant(to, TIMEZONE_OFFSET);

        Iterable<Interval> result = new ArrayList<>();

        if (fromDateTime.toLocalDate().compareTo(toDateTime.toLocalDate()) == 0)
        {
            return getIntervalsPerDay(from, to, crawlerName, crawlerVersion, crawlerType);
        }

        LocalDateTime point = fromDateTime;

        while (point.isBefore(toDateTime))
        {
            point = LocalDateTime.of(fromDateTime.toLocalDate(), LocalTime.MAX);

            if (point.isAfter(toDateTime))
                point = toDateTime;

            Iterable<Interval> intervals = getIntervalsPerDay(fromDateTime, point, crawlerName, crawlerVersion, crawlerType);

            fromDateTime = fromDateTime.plusDays(1).truncatedTo(ChronoUnit.DAYS);

            result = Iterables.concat(result, intervals);
        }

        return result;
    }

    @Override
    public Interval setIntervalLastUpdateTimeAndDate(Interval interval, Instant newLastUpdateTime) throws IOException
    {
        try
        {
            return setIntervalLastUpdateTimeAndDateAsync(interval, newLastUpdateTime).get();
        }
		catch (Exception e)
        {
            if (e instanceof ExecutionException && e.getCause() instanceof UpdateNotAppliedException) {
                throw (UpdateNotAppliedException) e.getCause();
            }
            throw new IOException("Error while occupying interval from: "+interval.getStartTime()+", to: "
                    +interval.getEndTime()+", name: "+interval.getCrawlerName()+", version: "
                    +interval.getCrawlerVersion()+", type: "+interval.getCrawlerType(), e);
        }
    }

    @Override
    public CompletableFuture<Interval> setIntervalLastUpdateTimeAndDateAsync(Interval interval, Instant newLastUpdateTime)
    {
        LocalDateTime dateTime = LocalDateTime.ofInstant(newLastUpdateTime, TIMEZONE_OFFSET);

        LocalTime time = dateTime.toLocalTime();
        LocalDate date = dateTime.toLocalDate();

        CompletableFuture<AsyncResultSet> future = new AsyncOperator<AsyncResultSet>(semaphore)
                .getFuture(() ->
                        intervalOperator.setIntervalLastUpdateTimeAndDate(instanceUuid,
                                LocalDate.from(interval.getStartTime().atOffset(TIMEZONE_OFFSET)),
                                LocalTime.from(interval.getStartTime().atOffset(TIMEZONE_OFFSET)), time, date,
                                LocalTime.from(interval.getLastUpdateDateTime().atOffset(TIMEZONE_OFFSET)),
                                LocalDate.from(interval.getLastUpdateDateTime().atOffset(TIMEZONE_OFFSET)),
                                interval.getCrawlerName(), interval.getCrawlerVersion(), interval.getCrawlerType(),
                                writeAttrs));
        return future.thenApply(result -> {
            if (!result.wasApplied()) {
                throw new UpdateNotAppliedException(
                        String.format("Cannot update the 'lastUpdateTime' column for interval from %s to %s (name: %s, version: %s)",
                                interval.getStartTime(), interval.getEndTime(), interval.getCrawlerName(), interval.getCrawlerVersion()
                        ));
            }
            return Interval.copyWith(interval, interval.getRecoveryState(), dateTime, interval.isProcessed());
        });
    }

    @Override
    public Interval updateRecoveryState(Interval interval, String recoveryState) throws IOException
    {
        try
        {
            return updateRecoveryStateAsync(interval, recoveryState).get();
        }
        catch (Exception e)
        {
            if (e instanceof ExecutionException && e.getCause() instanceof UpdateNotAppliedException) {
                throw (UpdateNotAppliedException) e.getCause();
            }
            throw new IOException("Error while updating recovery state of interval from: "+interval.getStartTime()+", to: "
                    +interval.getEndTime()+", name: "+interval.getCrawlerName()+", version: "
                    +interval.getCrawlerVersion()+", type: "+interval.getCrawlerType(), e);
        }
    }

    @Override
    public CompletableFuture<Interval> updateRecoveryStateAsync(Interval interval, String recoveryState)
    {
        LocalDateTime newLastUpdateDateTime = LocalDateTime.ofInstant(Instant.now(), TIMEZONE_OFFSET);

        LocalTime newLastUpdateTime = newLastUpdateDateTime.toLocalTime();
        LocalDate newLastUpdateDate = newLastUpdateDateTime.toLocalDate();

        CompletableFuture<AsyncResultSet> future = new AsyncOperator<AsyncResultSet>(semaphore)
                .getFuture(() -> intervalOperator.updateRecoveryState(instanceUuid,
                        LocalDate.from(interval.getStartTime().atOffset(TIMEZONE_OFFSET)),
                        LocalTime.from(interval.getStartTime().atOffset(TIMEZONE_OFFSET)),
                        newLastUpdateTime, newLastUpdateDate, recoveryState,
                        interval.getRecoveryState(),
                        LocalTime.from(interval.getLastUpdateDateTime().atOffset(TIMEZONE_OFFSET)),
                        LocalDate.from(interval.getLastUpdateDateTime().atOffset(TIMEZONE_OFFSET)),
                        interval.getCrawlerName(), interval.getCrawlerVersion(), interval.getCrawlerType(),
                        writeAttrs));

        return future.thenApply(result -> {
            if (!result.wasApplied()) {
                throw new UpdateNotAppliedException(
                        String.format("Cannot update the 'recovery state' column for interval from %s to %s (name: %s, version: %s)",
                                interval.getStartTime(), interval.getEndTime(), interval.getCrawlerName(), interval.getCrawlerVersion()
                        ));
            }
            return Interval.copyWith(interval, recoveryState, newLastUpdateDateTime, interval.isProcessed());
        });
    }

    @Override
    public Interval setIntervalProcessed(Interval interval, boolean processed) throws IOException
    {
        try
        {
            return setIntervalProcessedAsync(interval, processed).get();
        }
        catch (Exception e)
        {
            if (e instanceof ExecutionException && e.getCause() instanceof UpdateNotAppliedException) {
                throw (UpdateNotAppliedException) e.getCause();
            }
            throw new IOException("Error while setting processed flag of interval from: "+interval.getStartTime()+", to: "
                    +interval.getEndTime()+", name: "+interval.getCrawlerName()+", version: "
                    +interval.getCrawlerVersion()+", type: "+interval.getCrawlerType(), e);
        }
    }

    @Override
    public CompletableFuture<Interval> setIntervalProcessedAsync(Interval interval, boolean processed)
    {
        LocalDateTime newLastUpdateDateTime = LocalDateTime.ofInstant(Instant.now(), TIMEZONE_OFFSET);

        LocalTime newLastUpdateTime = newLastUpdateDateTime.toLocalTime();
        LocalDate newLastUpdateDate = newLastUpdateDateTime.toLocalDate();

        CompletableFuture<AsyncResultSet> future = new AsyncOperator<AsyncResultSet>(semaphore)
                .getFuture(() -> intervalOperator.setIntervalProcessed(instanceUuid,
                        LocalDate.from(interval.getStartTime().atOffset(TIMEZONE_OFFSET)),
                        LocalTime.from(interval.getStartTime().atOffset(TIMEZONE_OFFSET)),
                        newLastUpdateTime, newLastUpdateDate, processed, interval.isProcessed(),
                        LocalTime.from(interval.getLastUpdateDateTime().atOffset(TIMEZONE_OFFSET)),
                        LocalDate.from(interval.getLastUpdateDateTime().atOffset(TIMEZONE_OFFSET)),
                        interval.getCrawlerName(), interval.getCrawlerVersion(), interval.getCrawlerType(),
                        writeAttrs));

        return future.thenApply(result -> {
            if (!result.wasApplied()) {
                throw new UpdateNotAppliedException(
                        String.format("Cannot update the 'process' column for interval from %s to %s (name: %s, version: %s)",
                                interval.getStartTime(), interval.getEndTime(), interval.getCrawlerName(), interval.getCrawlerVersion()
                        ));
            }
            return Interval.copyWith(interval, interval.getRecoveryState(), newLastUpdateDateTime, processed);
        });
    }

    private void checkTimeBoundaries(LocalDateTime fromDateTime, LocalDateTime toDateTime, Instant originalFrom, Instant originalTo)
            throws CradleStorageException
    {
        LocalDate fromDate = fromDateTime.toLocalDate(),
                toDate = toDateTime.toLocalDate();
        if (!fromDate.equals(toDate))
            throw new CradleStorageException("Left and right boundaries should be of the same date, but got '"+originalFrom+"' and '"+originalTo+"'");
    }

    private void checkTimeBoundaries(LocalDateTime fromDateTime, LocalDateTime toDateTime) throws CradleStorageException {
        LocalDate fromDate = fromDateTime.toLocalDate(),
                toDate = toDateTime.toLocalDate();
        if (!fromDate.equals(toDate))
            throw new CradleStorageException("Left and right boundaries should be of the same date, but got '"+fromDateTime+"' and '"+toDateTime+"'");
    }
}

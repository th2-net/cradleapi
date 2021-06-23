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
    public boolean storeInterval(Interval interval) throws IOException
    {
        try
        {
            return storeIntervalAsync(interval).get();
        }
        catch (Exception e)
        {
            throw new IOException("Error while storing interval "+interval.getId(), e);
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
    public Iterable<Interval> getIntervals(Instant from, Instant to, String crawlerName, String crawlerVersion) throws IOException
    {
        try
        {
            return getIntervalsAsync(from, to, crawlerName, crawlerVersion).get();
        }
        catch (Exception e)
        {
            throw new IOException("Error while getting intervals from: "+from+", to: "+to+" by Crawler with " +
                    "name: "+crawlerName+", version: "+crawlerVersion, e);
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
                throw new CompletionException("Could not get intervals from: "+from+", to: "+to+" by Crawler with " +
                        "name: "+crawlerName+", version: "+crawlerVersion, error);
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
                        intervalOperator.setIntervalLastUpdateTimeAndDate(instanceUuid,
                                interval.getStartDateTime().toLocalDate(),
                                interval.getStartDateTime().toLocalTime(), time, date,
                                interval.getLastUpdateDateTime().toLocalTime(),
                                interval.getLastUpdateDateTime().toLocalDate(),
                                interval.getCrawlerName(), interval.getCrawlerVersion(),
                                writeAttrs));
        return future.thenApply(AsyncResultSet::wasApplied);
    }

    @Override
    public boolean updateRecoveryState(Interval interval, RecoveryState recoveryState) throws IOException
    {
        try
        {
            return updateRecoveryStateAsync(interval, recoveryState).get();
        }
        catch (Exception e)
        {
            throw new IOException("Error while updating recovery state of interval "+interval.getId(), e);
        }
    }

    @Override
    public CompletableFuture<Boolean> updateRecoveryStateAsync(Interval interval, RecoveryState recoveryState)
    {
        LocalDateTime newLastUpdateDateTime = LocalDateTime.ofInstant(Instant.now(), TIMEZONE_OFFSET);

        LocalTime newLastUpdateTime = newLastUpdateDateTime.toLocalTime();
        LocalDate newLastUpdateDate = newLastUpdateDateTime.toLocalDate();

        CompletableFuture<AsyncResultSet> future = new AsyncOperator<AsyncResultSet>(semaphore)
                .getFuture(() -> intervalOperator.updateRecoveryState(instanceUuid,
                        interval.getStartDateTime().toLocalDate(), interval.getStartDateTime().toLocalTime(),
                        newLastUpdateTime, newLastUpdateDate, recoveryState.convertToJson(),
                        interval.getRecoveryState().convertToJson(),
                        interval.getLastUpdateDateTime().toLocalTime(),
                        interval.getLastUpdateDateTime().toLocalDate(),
                        interval.getCrawlerName(), interval.getCrawlerVersion(),
                        writeAttrs));

        return future.thenApply(AsyncResultSet::wasApplied);
    }

    @Override
    public boolean setIntervalProcessed(Interval interval, boolean processed) throws IOException
    {
        try
        {
            return setIntervalProcessedAsync(interval, processed).get();
        }
        catch (Exception e)
        {
            throw new IOException("Error while setting processed flag of interval "+interval.getId(), e);
        }
    }

    @Override
    public CompletableFuture<Boolean> setIntervalProcessedAsync(Interval interval, boolean processed)
    {
        LocalDateTime newLastUpdateDateTime = LocalDateTime.ofInstant(Instant.now(), TIMEZONE_OFFSET);

        LocalTime newLastUpdateTime = newLastUpdateDateTime.toLocalTime();
        LocalDate newLastUpdateDate = newLastUpdateDateTime.toLocalDate();

        CompletableFuture<AsyncResultSet> future = new AsyncOperator<AsyncResultSet>(semaphore)
                .getFuture(() -> intervalOperator.setIntervalProcessed(instanceUuid,
                        interval.getStartDateTime().toLocalDate(), interval.getStartDateTime().toLocalTime(),
                        newLastUpdateTime, newLastUpdateDate, processed, interval.isProcessed(),
                        interval.getLastUpdateDateTime().toLocalTime(),
                        interval.getLastUpdateDateTime().toLocalDate(),
                        interval.getCrawlerName(), interval.getCrawlerVersion(),
                        writeAttrs));

        return future.thenApply(AsyncResultSet::wasApplied);
    }
}

/*
 * Copyright 2020-2022 Exactpro (Exactpro Systems Limited)
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
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.shaded.guava.common.collect.Iterables;
import com.exactpro.cradle.BookId;
import com.exactpro.cradle.cassandra.EntityConverter;
import com.exactpro.cradle.cassandra.iterators.PagedIterator;
import com.exactpro.cradle.cassandra.workers.Worker;
import com.exactpro.cradle.cassandra.workers.WorkerSupplies;
import com.exactpro.cradle.intervals.Interval;
import com.exactpro.cradle.intervals.IntervalsWorker;
import com.exactpro.cradle.iterators.ConvertingIterator;
import com.exactpro.cradle.utils.CradleStorageException;
import com.exactpro.cradle.utils.UpdateNotAppliedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

import static com.exactpro.cradle.CradleStorage.TIMEZONE_OFFSET;

public class CassandraIntervalsWorker extends Worker implements IntervalsWorker {

    private static final Logger logger = LoggerFactory.getLogger(CassandraIntervalsWorker.class);

    private final IntervalOperator operator;
    private final EntityConverter<IntervalEntity> converter;
    private final Function<BoundStatementBuilder, BoundStatementBuilder> readAttrs, writeAttrs;

    public CassandraIntervalsWorker(WorkerSupplies workerSupplies) {
        super(workerSupplies);
        this.operator = workerSupplies.getOperators().getIntervalOperator();
        this.converter = workerSupplies.getOperators().getIntervalEntityConverter();
        this.readAttrs = workerSupplies.getReadAttrs();
        this.writeAttrs = workerSupplies.getWriteAttrs();
    }

    public static Interval entityToInterval(IntervalEntity entity) throws IOException {
        return Interval.builder()
                .setBookId(new BookId(entity.getBook()))
                .setStart(LocalDateTime.of(entity.getStartDate(), entity.getStartTime()).atOffset(TIMEZONE_OFFSET).toInstant())
                .setEnd(LocalDateTime.of(entity.getEndDate(), entity.getEndTime()).atOffset(TIMEZONE_OFFSET).toInstant())
                .setLastUpdate(LocalDateTime.of(entity.getLastUpdateDate(), entity.getLastUpdateTime()).atOffset(TIMEZONE_OFFSET).toInstant())
                .setRecoveryState(entity.getRecoveryState())
                .setCrawlerName(entity.getCrawlerName())
                .setCrawlerVersion(entity.getCrawlerVersion())
                .setCrawlerType(entity.getCrawlerType())
                .setProcessed(entity.isProcessed())
                .build();
    }

    private Interval mapEntityToInterval (IntervalEntity entity) {
        try {
            return entityToInterval(entity);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Iterable<Interval> getIntervalsIterator(MappedAsyncPagingIterable<IntervalEntity> iterable, String queryInfo) {
        return () -> {
            PagedIterator<IntervalEntity> pagedIterator = new PagedIterator<>(iterable,
                    selectQueryExecutor,
                    converter::getEntity,
                    queryInfo);

            return new ConvertingIterator<>(
                    pagedIterator,
                    this::mapEntityToInterval);
        };

    }

    @Override
    public boolean storeInterval(Interval interval) throws CradleStorageException {
        String queryInfo = String.format("Storing interval for crawler %s:%s in  book %s",
                interval.getCrawlerName(),
                interval.getCrawlerType(),
                interval.getBookId());
        try {
            return storeIntervalAsync(interval).get();
        } catch (InterruptedException | ExecutionException e) {
            logger.error("Exception while {}", queryInfo);
            throw new CradleStorageException(queryInfo, e);
        }
    }

    @Override
    public CompletableFuture<Boolean> storeIntervalAsync(Interval interval) {
        String queryInfo = String.format("Storing interval for crawler %s:%s in book %s",
                interval.getCrawlerName(),
                interval.getCrawlerType(),
                interval.getBookId());

        IntervalEntity.IntervalEntityBuilder builder = IntervalEntity.builder()
                .setBook(interval.getBookId().getName())
                .setStartDate(LocalDate.ofInstant(interval.getStart(), TIMEZONE_OFFSET))
                .setStartTime(LocalTime.ofInstant(interval.getStart(), TIMEZONE_OFFSET))
                .setCrawlerName(interval.getCrawlerName())
                .setCrawlerType(interval.getCrawlerType())
                .setCrawlerVersion(interval.getCrawlerVersion())
                .setEndDate(LocalDate.ofInstant(interval.getEnd(), TIMEZONE_OFFSET))
                .setEndTime(LocalTime.ofInstant(interval.getEnd(), TIMEZONE_OFFSET))
                .setLastUpdateDate(LocalDate.ofInstant(interval.getLastUpdate(), TIMEZONE_OFFSET))
                .setLastUpdateTime(LocalTime.ofInstant(interval.getLastUpdate(), TIMEZONE_OFFSET))
                .setRecoveryState(interval.getRecoveryState())
                .setProcessed(interval.isProcessed());



        IntervalEntity intervalEntity = builder.build();
        
        logger.debug(queryInfo);

        return operator.writeInterval(intervalEntity, writeAttrs);
    }

    @Override
    public Iterable<Interval> getIntervalsPerDay(BookId bookId, Instant from, Instant to, String crawlerName, String crawlerVersion, String crawlerType) throws CradleStorageException {
        LocalDate date = LocalDate.ofInstant(from, TIMEZONE_OFFSET);
        LocalTime fromTime = LocalTime.ofInstant(from, TIMEZONE_OFFSET);
        LocalTime toTime = LocalTime.ofInstant(to, TIMEZONE_OFFSET);

        String queryInfo = String.format("Getting intervals for crawler %s:%s for day %s between times %s-%s in book %s",
                crawlerName,
                crawlerType,
                date,
                fromTime,
                toTime,
                bookId);

        try {
            return getIntervalsPerDayAsync(bookId, from, to, crawlerName, crawlerVersion, crawlerType).get();
        } catch (CradleStorageException | InterruptedException | ExecutionException e) {
            logger.error("Exception while {}", queryInfo);
            throw new CradleStorageException(queryInfo, e);
        }
    }

    @Override
    public CompletableFuture<Iterable<Interval>> getIntervalsPerDayAsync(BookId bookId, Instant from, Instant to, String crawlerName, String crawlerVersion, String crawlerType) throws CradleStorageException {
        LocalDate date = LocalDate.ofInstant(from, TIMEZONE_OFFSET);
        LocalTime fromTime = LocalTime.ofInstant(from, TIMEZONE_OFFSET);
        LocalTime toTime = LocalTime.ofInstant(to, TIMEZONE_OFFSET);

        String queryInfo = String.format("Getting intervals for crawler %s:%s for day %s between times %s-%s in book %s",
                crawlerName,
                crawlerType,
                date,
                fromTime,
                toTime,
                bookId);

        return selectQueryExecutor.executeMappedMultiRowResultQuery(
                () -> operator.getIntervals(bookId.getName(), date, fromTime, toTime, crawlerName, crawlerVersion, crawlerType, readAttrs),
                converter::getEntity,
                queryInfo)
                .thenApplyAsync((result) ->  getIntervalsIterator(result, queryInfo));
    }

    @Override
    public Iterable<Interval> getIntervals(BookId bookId, Instant from, Instant to, String crawlerName, String crawlerVersion, String crawlerType) throws CradleStorageException {
        String queryInfo = String.format("Getting intervals for crawler %s:%s between timestamps %s-%s in book %s",
                crawlerName,
                crawlerType,
                from,
                to,
                bookId);

        logger.debug(queryInfo);

        try {
            LocalDateTime fromDateTime = LocalDateTime.ofInstant(from, TIMEZONE_OFFSET),
                    toDateTime = LocalDateTime.ofInstant(to, TIMEZONE_OFFSET);

            Iterable<Interval> result = new ArrayList<>();

            if (fromDateTime.toLocalDate().compareTo(toDateTime.toLocalDate()) == 0)
            {
                return getIntervalsPerDay(bookId, from, to, crawlerName, crawlerVersion, crawlerType);
            }

            LocalDateTime point = fromDateTime;

            while (point.isBefore(toDateTime))
            {
                point = LocalDateTime.of(fromDateTime.toLocalDate(), LocalTime.MAX);

                if (point.isAfter(toDateTime))
                    point = toDateTime;

                Iterable<Interval> intervals = getIntervalsPerDay(bookId, fromDateTime, point, crawlerName, crawlerVersion, crawlerType);

                fromDateTime = fromDateTime.plusDays(1).truncatedTo(ChronoUnit.DAYS);

                result = Iterables.concat(result, intervals);
            }

            return result;
        } catch (CradleStorageException e) {
            logger.error("Error while {}", queryInfo);
            throw e;
        }
    }

    private Iterable<Interval> getIntervalsPerDay(BookId bookId, LocalDateTime from, LocalDateTime to, String crawlerName, String crawlerVersion, String crawlerType) throws CradleStorageException {
        LocalTime fromTime = from.toLocalTime();
        LocalTime toTime = to.toLocalTime();
        LocalDate date = from.toLocalDate();

        String queryInfo = String.format("Getting intervals for crawler %s:%s for day %s between times %s-%s in book %s",
                crawlerName,
                crawlerType,
                date,
                fromTime,
                toTime,
                bookId);

        try
        {
            return getIntervalsPerDayAsync(bookId, from, to, crawlerName, crawlerVersion, crawlerType).get();
        } catch (CradleStorageException | InterruptedException | ExecutionException e) {
            logger.error("Exception while {}", queryInfo);
            throw new CradleStorageException(queryInfo, e);
        }
    }

    private CompletableFuture<Iterable<Interval>> getIntervalsPerDayAsync(BookId bookId, LocalDateTime from, LocalDateTime to, String crawlerName,
                                                                          String crawlerVersion, String crawlerType) throws CradleStorageException {

        checkTimeBoundaries(from, to);

        LocalTime fromTime = from.toLocalTime();
        LocalTime toTime = to.toLocalTime();
        LocalDate date = from.toLocalDate();

        String queryInfo = String.format("Getting intervals for crawler %s:%s for day %s between times %s-%s in book %s",
                crawlerName,
                crawlerType,
                date,
                fromTime,
                toTime,
                bookId);

        return selectQueryExecutor.executeMappedMultiRowResultQuery(
                        () -> operator.getIntervals(bookId.getName(), date, fromTime, toTime, crawlerName, crawlerVersion, crawlerType, readAttrs),
                        converter::getEntity,
                        queryInfo)
                .thenApplyAsync((result) ->  getIntervalsIterator(result, queryInfo));
    }

    @Override
    public Interval setIntervalLastUpdateTimeAndDate(Interval interval, Instant newLastUpdateTime) throws CradleStorageException {
        String queryInfo = String.format("Setting last update date and time as %s for interval %s in book %s",
                newLastUpdateTime,
                interval,
                interval.getBookId());

        try {
            return setIntervalLastUpdateTimeAndDateAsync(interval, newLastUpdateTime).get();
        } catch (InterruptedException | ExecutionException e) {
            logger.error("Exception while {}", queryInfo);
            throw new CradleStorageException(queryInfo, e);
        }
    }

    @Override
    public CompletableFuture<Interval> setIntervalLastUpdateTimeAndDateAsync(Interval interval, Instant newLastUpdateTime) {
        String queryInfo = String.format("Setting last update date and time as %s for interval %s in book %s",
                newLastUpdateTime,
                interval,
                interval.getBookId());

        logger.debug(queryInfo);

        LocalDateTime dateTime = LocalDateTime.ofInstant(newLastUpdateTime, TIMEZONE_OFFSET);

        LocalDate startDate = LocalDate.ofInstant(interval.getStart(), TIMEZONE_OFFSET);
        LocalTime startTime = LocalTime.ofInstant(interval.getStart(), TIMEZONE_OFFSET);

        LocalDate newDate = dateTime.toLocalDate();
        LocalTime newTime = dateTime.toLocalTime();

        LocalDate oldUpdateDate = LocalDate.from(interval.getLastUpdate().atOffset(TIMEZONE_OFFSET));
        LocalTime oldUpdateTime = LocalTime.from(interval.getLastUpdate().atOffset(TIMEZONE_OFFSET));

        return operator.setIntervalLastUpdateTimeAndDate(
                interval.getBookId().getName(),
                startDate, startTime,
                newTime, newDate,
                oldUpdateTime, oldUpdateDate,
                interval.getCrawlerName(),
                interval.getCrawlerVersion(),
                interval.getCrawlerType(), writeAttrs).thenApply(result -> {
                    if (!result.wasApplied()) {
                        throw new UpdateNotAppliedException("Update wasn't applied for : " + queryInfo);
                    }

                    return copyWith(interval, interval.getRecoveryState(), dateTime, interval.isProcessed());
        });
    }

    @Override
    public Interval updateRecoveryState(Interval interval, String recoveryState) throws CradleStorageException {
        String queryInfo = String.format("Updating recoveryState to %s for interval %s in book %s",
                recoveryState,
                interval,
                interval.getBookId());

        try {
            return updateRecoveryStateAsync(interval, recoveryState).get();
        } catch (InterruptedException | ExecutionException e) {
            logger.error("Exception while {}", queryInfo);
            throw new CradleStorageException(queryInfo, e);
        }
    }

    @Override
    public CompletableFuture<Interval> updateRecoveryStateAsync(Interval interval, String recoveryState) {
        String queryInfo = String.format("Updating recoveryState to %s for interval %s in book %s",
                recoveryState,
                interval,
                interval.getBookId());

        LocalDateTime newLastUpdateDateTime = LocalDateTime.ofInstant(Instant.now(), TIMEZONE_OFFSET);

        LocalTime newLastUpdateTime = newLastUpdateDateTime.toLocalTime();
        LocalDate newLastUpdateDate = newLastUpdateDateTime.toLocalDate();

        LocalDate startDate = LocalDate.ofInstant(interval.getStart(), TIMEZONE_OFFSET);
        LocalTime startTime = LocalTime.ofInstant(interval.getStart(), TIMEZONE_OFFSET);

        LocalDate oldUpdateDate = LocalDate.from(interval.getLastUpdate().atOffset(TIMEZONE_OFFSET));
        LocalTime oldUpdateTime = LocalTime.from(interval.getLastUpdate().atOffset(TIMEZONE_OFFSET));

        return operator.updateRecoveryState(interval.getBookId().getName(),
                startDate, startTime,
                newLastUpdateTime, newLastUpdateDate,
                recoveryState, interval.getRecoveryState(),
                oldUpdateTime, oldUpdateDate,
                interval.getCrawlerName(),
                interval.getCrawlerVersion(),
                interval.getCrawlerType(), writeAttrs).thenApply((result) -> {
                    if (!result.wasApplied()) {
                        throw new UpdateNotAppliedException("Update wasn't applied for : " + queryInfo);
                    }

                return copyWith(interval, recoveryState, newLastUpdateDateTime, interval.isProcessed());
        });

    }

    @Override
    public Interval setIntervalProcessed(Interval interval, boolean processed) throws CradleStorageException {
        String queryInfo = String.format("Updating processed to %s for interval %s in book %s",
                processed,
                interval,
                interval.getBookId());

        try {
            return setIntervalProcessedAsync(interval, processed).get();
        } catch (InterruptedException | ExecutionException e) {
            logger.error("Exception while {}", queryInfo);
            throw new CradleStorageException(queryInfo, e);
        }
    }

    @Override
    public CompletableFuture<Interval> setIntervalProcessedAsync(Interval interval, boolean processed) {
        String queryInfo = String.format("Updating processed to %s for interval %s in book %s",
                processed,
                interval,
                interval.getBookId());

        LocalDateTime newLastUpdateDateTime = LocalDateTime.ofInstant(Instant.now(), TIMEZONE_OFFSET);

        LocalTime newLastUpdateTime = newLastUpdateDateTime.toLocalTime();
        LocalDate newLastUpdateDate = newLastUpdateDateTime.toLocalDate();


        LocalDate startDate = LocalDate.ofInstant(interval.getStart(), TIMEZONE_OFFSET);
        LocalTime startTime = LocalTime.ofInstant(interval.getStart(), TIMEZONE_OFFSET);

        LocalDate oldUpdateDate = LocalDate.from(interval.getLastUpdate().atOffset(TIMEZONE_OFFSET));
        LocalTime oldUpdateTime = LocalTime.from(interval.getLastUpdate().atOffset(TIMEZONE_OFFSET));

        return operator.setIntervalProcessed(interval.getBookId().getName(),
                startDate, startTime,
                newLastUpdateTime, newLastUpdateDate,
                processed, interval.isProcessed(),
                oldUpdateTime, oldUpdateDate,
                interval.getCrawlerName(),
                interval.getCrawlerVersion(),
                interval.getCrawlerType(),
                writeAttrs).thenApply((result) -> {
                    if (!result.wasApplied()) {
                        throw new UpdateNotAppliedException("Update wasn't applied for : " + queryInfo);
                    }

                    return copyWith(interval, interval.getRecoveryState(), newLastUpdateDateTime, processed);
                    });
    }

    private void checkTimeBoundaries(LocalDateTime fromDateTime, LocalDateTime toDateTime) throws CradleStorageException {
        LocalDate fromDate = fromDateTime.toLocalDate(),
                toDate = toDateTime.toLocalDate();
        if (!fromDate.equals(toDate))
            throw new CradleStorageException("Left and right boundaries should be of the same date, but got '"+fromDateTime+"' and '"+toDateTime+"'");
    }

    private static Interval copyWith(Interval original, String recoveryState, LocalDateTime lastUpdate, boolean processed) {
        Interval.IntervalBuilder builder = Interval.builder(original);

        builder.setRecoveryState(recoveryState);
        builder.setLastUpdate(lastUpdate.toInstant(TIMEZONE_OFFSET));
        builder.setProcessed(processed);

        return builder.build();
    }
}

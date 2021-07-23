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

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.shaded.guava.common.collect.Iterables;
import com.exactpro.cradle.cassandra.iterators.IntervalsIteratorAdapter;
import com.exactpro.cradle.cassandra.retry.AsyncExecutor;
import com.exactpro.cradle.cassandra.retry.SyncExecutor;
import com.exactpro.cradle.cassandra.utils.DateTimeUtils;
import com.exactpro.cradle.cassandra.utils.TimestampRange;
import com.exactpro.cradle.exceptions.CradleStorageException;
import com.exactpro.cradle.exceptions.TooManyRequestsException;
import com.exactpro.cradle.exceptions.UpdateNotAppliedException;
import com.exactpro.cradle.intervals.Interval;
import com.exactpro.cradle.intervals.IntervalsWorker;
import com.exactpro.cradle.intervals.RecoveryState;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

public class CassandraIntervalsWorker implements IntervalsWorker
{
	private final UUID instanceUuid;
	private final Function<BoundStatementBuilder, BoundStatementBuilder> writeAttrs, readAttrs;
	private final IntervalOperator intervalOperator;
	private final SyncExecutor syncExecutor;
	private final AsyncExecutor asyncExecutor;

	public CassandraIntervalsWorker(UUID instanceUuid, Function<BoundStatementBuilder, BoundStatementBuilder> writeAttrs,
			Function<BoundStatementBuilder, BoundStatementBuilder> readAttrs, IntervalOperator intervalOperator,
			SyncExecutor syncExecutor, AsyncExecutor asyncExecutor)
	{
		this.instanceUuid = instanceUuid;
		this.writeAttrs = writeAttrs;
		this.readAttrs = readAttrs;
		this.intervalOperator = intervalOperator;
		this.syncExecutor = syncExecutor;
		this.asyncExecutor = asyncExecutor;
	}

	@Override
	public boolean storeInterval(Interval interval) throws IOException
	{
		try
		{
			return syncExecutor.submit("store interval "+interval.getStartTime()+".."+interval.getEndTime(), 
					() -> writeInterval(interval));
		}
		catch (Exception e)
		{
			throw new IOException("Error while storing interval from: " + interval.getStartTime() + ", to: "
					+ interval.getEndTime() + ", name: " + interval.getCrawlerName() + ", version: "
					+ interval.getCrawlerVersion() + ", type: " + interval.getCrawlerType(), e);
		}
	}

	@Override
	public CompletableFuture<Boolean> storeIntervalAsync(Interval interval) throws TooManyRequestsException
	{
		return asyncExecutor.submit("store interval "+interval.getStartTime()+".."+interval.getEndTime(), 
				() -> writeInterval(interval));
	}

	@Override
	public Iterable<Interval> getIntervalsPerDay(Instant from, Instant to, String crawlerName, String crawlerVersion,
			String crawlerType) throws IOException
	{
		try
		{
			TimestampRange range = new TimestampRange(from, to);
			return syncExecutor.submit("get intervals from "+from+" to "+to, 
					() -> readIntervals(range, crawlerName, crawlerVersion, crawlerType));
		} catch (Exception e)
		{
			throw new IOException("Error while getting intervals from: " + from + ", to: " + to + " by Crawler with "
					+ "name: " + crawlerName + ", version: " + crawlerVersion + ", type: " + crawlerType, e);
		}
	}

	@Override
	public CompletableFuture<Iterable<Interval>> getIntervalsPerDayAsync(Instant from, Instant to, String crawlerName,
			String crawlerVersion, String crawlerType) throws CradleStorageException, TooManyRequestsException
	{
		TimestampRange range = new TimestampRange(from, to);
		return asyncExecutor.submit("get intervals from "+from+" to "+to, 
				() -> readIntervals(range, crawlerName, crawlerVersion, crawlerType));
	}

	@Override
	public Iterable<Interval> getIntervals(Instant from, Instant to, String crawlerName, String crawlerVersion,
			String crawlerType) throws IOException
	{
		LocalDateTime fromDateTime = DateTimeUtils.toDateTime(from),
				toDateTime = DateTimeUtils.toDateTime(to);
		
		if (fromDateTime.toLocalDate().compareTo(toDateTime.toLocalDate()) == 0)
			return getIntervalsPerDay(from, to, crawlerName, crawlerVersion, crawlerType);
		
		LocalDateTime point = fromDateTime;
		
		Iterable<Interval> result = new ArrayList<>();
		while (point.isBefore(toDateTime))
		{
			point = LocalDateTime.of(fromDateTime.toLocalDate(), LocalTime.MAX);
			
			if (point.isAfter(toDateTime))
				point = toDateTime;
			
			Iterable<Interval> intervals;
			try
			{
				LocalDateTime fdt = fromDateTime,
						tdt = point;
				intervals = syncExecutor.submit("get Crawler intervals from "+fromDateTime+" to "+point, 
						() -> readIntervals(fdt, tdt, crawlerName, crawlerVersion, crawlerType));
			}
			catch (Exception e)
			{
				throw new IOException("Error while getting intervals from: " + fromDateTime + ", to: " + point + " by Crawler with "
						+ "name: " + crawlerName + ", version: " + crawlerVersion + ", type: " + crawlerType, e);
			}
			
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
			return syncExecutor.submit("set update timestamp of interval "+interval.getStartTime()+".."+interval.getEndTime(), 
					() -> writeIntervalUpdateTimestamp(interval, newLastUpdateTime));
		}
		catch (Exception e)
		{
			if (e instanceof ExecutionException && e.getCause() instanceof UpdateNotAppliedException)
			{
				throw (UpdateNotAppliedException) e.getCause();
			}
			throw new IOException("Error while occupying interval from: " + interval.getStartTime() + ", to: "
					+ interval.getEndTime() + ", name: " + interval.getCrawlerName() + ", version: "
					+ interval.getCrawlerVersion() + ", type: " + interval.getCrawlerType(), e);
		}
	}

	@Override
	public CompletableFuture<Interval> setIntervalLastUpdateTimeAndDateAsync(Interval interval, Instant newLastUpdateTime) throws TooManyRequestsException
	{
		return asyncExecutor.submit("set update timestamp of interval "+interval.getStartTime()+".."+interval.getEndTime(), 
				() -> writeIntervalUpdateTimestamp(interval, newLastUpdateTime));
	}

	@Override
	public Interval updateRecoveryState(Interval interval, RecoveryState recoveryState) throws IOException
	{
		try
		{
			return syncExecutor.submit("update recovery state of interval "+interval.getStartTime()+".."+interval.getEndTime(), 
					() -> writeRecoveryState(interval, recoveryState));
		}
		catch (Exception e)
		{
			if (e instanceof ExecutionException && e.getCause() instanceof UpdateNotAppliedException)
			{
				throw (UpdateNotAppliedException) e.getCause();
			}
			throw new IOException("Error while updating recovery state of interval from: " + interval.getStartTime()
					+ ", to: " + interval.getEndTime() + ", name: " + interval.getCrawlerName() + ", version: "
					+ interval.getCrawlerVersion() + ", type: " + interval.getCrawlerType(), e);
		}
	}

	@Override
	public CompletableFuture<Interval> updateRecoveryStateAsync(Interval interval, RecoveryState recoveryState) throws TooManyRequestsException
	{
		return asyncExecutor.submit("update recovery state of interval "+interval.getStartTime()+".."+interval.getEndTime(), 
				() -> writeRecoveryState(interval, recoveryState));
	}

	@Override
	public Interval setIntervalProcessed(Interval interval, boolean processed) throws IOException
	{
		try
		{
			return syncExecutor.submit("set processed flag of interval "+interval.getStartTime()+".."+interval.getEndTime(), 
					() -> writeIntervalProcessed(interval, processed));
		} catch (Exception e)
		{
			if (e instanceof ExecutionException && e.getCause() instanceof UpdateNotAppliedException)
			{
				throw (UpdateNotAppliedException) e.getCause();
			}
			throw new IOException("Error while setting processed flag of interval from: " + interval.getStartTime() + ", to: "
					+ interval.getEndTime() + ", name: " + interval.getCrawlerName() + ", version: "
					+ interval.getCrawlerVersion() + ", type: " + interval.getCrawlerType(), e);
		}
	}

	@Override
	public CompletableFuture<Interval> setIntervalProcessedAsync(Interval interval, boolean processed) throws TooManyRequestsException
	{
		return asyncExecutor.submit("set processed flag of interval "+interval.getStartTime()+".."+interval.getEndTime(), 
				() -> writeIntervalProcessed(interval, processed));
	}
	
	
	private CompletableFuture<Boolean> writeInterval(Interval interval)
	{
		return intervalOperator.writeInterval(new IntervalEntity(interval, instanceUuid), writeAttrs)
				.thenApply(AsyncResultSet::wasApplied);
	}
	
	private CompletableFuture<Iterable<Interval>> readIntervals(LocalDateTime from, LocalDateTime to, String crawlerName, String crawlerVersion,
			String crawlerType)
	{
		LocalTime fromTime = from.toLocalTime(), 
				toTime = to.toLocalTime();
		LocalDate date = from.toLocalDate();
		return intervalOperator.getIntervals(instanceUuid, date, fromTime, toTime, crawlerName, crawlerVersion, crawlerType, readAttrs)
				.thenApply(IntervalsIteratorAdapter::new);
	}
	
	private CompletableFuture<Iterable<Interval>> readIntervals(TimestampRange range, String crawlerName, String crawlerVersion,
			String crawlerType)
	{
		return readIntervals(range.getFrom(), range.getTo(), crawlerName, crawlerVersion, crawlerType);
	}
	
	private CompletableFuture<Interval> writeIntervalUpdateTimestamp(Interval interval, Instant newLastUpdateTime)
	{
		LocalDateTime dateTime = DateTimeUtils.toDateTime(newLastUpdateTime);
		
		LocalTime time = dateTime.toLocalTime();
		LocalDate date = dateTime.toLocalDate();
		
		OffsetDateTime startDateTime = DateTimeUtils.atOffset(interval.getStartTime()),
				lastDateTime = DateTimeUtils.atOffset(interval.getLastUpdateDateTime());
		
		return intervalOperator.setIntervalLastUpdateTimeAndDate(instanceUuid,
				LocalDate.from(startDateTime),
				LocalTime.from(startDateTime), time, date,
				LocalTime.from(lastDateTime),
				LocalDate.from(lastDateTime), 
				interval.getCrawlerName(), interval.getCrawlerVersion(), interval.getCrawlerType(), writeAttrs)
				.thenApply(result -> {
					if (!result.wasApplied())
					{
						throw new UpdateNotAppliedException(String.format(
								"Cannot update the 'lastUpdateTime' column for interval from %s to %s (name: %s, version: %s)",
								interval.getStartTime(), interval.getEndTime(), interval.getCrawlerName(), interval.getCrawlerVersion()));
					}
					return Interval.copyWith(interval, interval.getRecoveryState(), dateTime, interval.isProcessed());
				});
	}
	
	private CompletableFuture<Interval> writeRecoveryState(Interval interval, RecoveryState recoveryState)
	{
		LocalDateTime newLastUpdateDateTime = DateTimeUtils.toDateTime(Instant.now());
		
		LocalTime newLastUpdateTime = newLastUpdateDateTime.toLocalTime();
		LocalDate newLastUpdateDate = newLastUpdateDateTime.toLocalDate();
		
		OffsetDateTime startDateTime = DateTimeUtils.atOffset(interval.getStartTime()),
				lastDateTime = DateTimeUtils.atOffset(interval.getLastUpdateDateTime());
		
		return intervalOperator.updateRecoveryState(instanceUuid,
				LocalDate.from(startDateTime),
				LocalTime.from(startDateTime), newLastUpdateTime, newLastUpdateDate,
				recoveryState.convertToJson(), interval.getRecoveryState().convertToJson(),
				LocalTime.from(lastDateTime),
				LocalDate.from(lastDateTime), 
				interval.getCrawlerName(), interval.getCrawlerVersion(), interval.getCrawlerType(), writeAttrs)
				.thenApply(result -> {
					if (!result.wasApplied())
					{
						throw new UpdateNotAppliedException(String.format(
								"Cannot update the 'recovery state' column for interval from %s to %s (name: %s, version: %s)",
								interval.getStartTime(), interval.getEndTime(), interval.getCrawlerName(), interval.getCrawlerVersion()));
					}
					return Interval.copyWith(interval, recoveryState, newLastUpdateDateTime, interval.isProcessed());
				});
	}
	
	private CompletableFuture<Interval> writeIntervalProcessed(Interval interval, boolean processed)
	{
		LocalDateTime newLastUpdateDateTime = DateTimeUtils.toDateTime(Instant.now());
		
		LocalTime newLastUpdateTime = newLastUpdateDateTime.toLocalTime();
		LocalDate newLastUpdateDate = newLastUpdateDateTime.toLocalDate();
		
		OffsetDateTime startDateTime = DateTimeUtils.atOffset(interval.getStartTime()),
				lastDateTime = DateTimeUtils.atOffset(interval.getLastUpdateDateTime());
		
		return intervalOperator.setIntervalProcessed(instanceUuid,
				LocalDate.from(startDateTime),
				LocalTime.from(startDateTime), newLastUpdateTime, newLastUpdateDate,
				processed, interval.isProcessed(),
				LocalTime.from(lastDateTime),
				LocalDate.from(lastDateTime), 
				interval.getCrawlerName(), interval.getCrawlerVersion(), interval.getCrawlerType(), writeAttrs)
				.thenApply(result -> {
					if (!result.wasApplied())
					{
						throw new UpdateNotAppliedException(String.format(
								"Cannot update the 'process' column for interval from %s to %s (name: %s, version: %s)",
								interval.getStartTime(), interval.getEndTime(), interval.getCrawlerName(), interval.getCrawlerVersion()));
					}
					return Interval.copyWith(interval, interval.getRecoveryState(), newLastUpdateDateTime, processed);
				});
	}
}

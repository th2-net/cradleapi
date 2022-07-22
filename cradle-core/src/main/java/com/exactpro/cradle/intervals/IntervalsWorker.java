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

package com.exactpro.cradle.intervals;

import com.exactpro.cradle.utils.CradleStorageException;

import java.io.IOException;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;

public interface IntervalsWorker
{
    /**
     * Writes to storage interval
     * @param interval data to write
     * @return true if interval was stored, false otherwise
     * @throws IOException if data writing failed
     */
    boolean storeInterval(Interval interval) throws IOException;

    /**
     * Asynchronously writes to storage interval
     * @param interval stored interval
     * @return future to get know if storing was successful
     */
    CompletableFuture<Boolean> storeIntervalAsync(Interval interval);

    /**
     * Obtains iterable of intervals with startTime greater than or equal to "from" and less than or equal to "to". Intervals must be within one day
     * @param from time from which intervals are searched
     * @param to time to which intervals are searched
     * @param crawlerName name of Crawler
     * @param crawlerVersion version of Crawler
     * @param crawlerType type of Crawler
     * @return iterable of intervals
     * @throws IOException if data retrieval failed
     */
    Iterable<Interval> getIntervalsPerDay(Instant from, Instant to, String crawlerName, String crawlerVersion, String crawlerType) throws IOException;

    /**
     * Asynchronously obtains iterable of intervals with startTime greater than or equal to "from" and less than or equal to "to". Intervals must be within one day
     * @param from time from which intervals are searched
     * @param to time to which intervals are searched
     * @param crawlerName name of Crawler
     * @param crawlerVersion version of Crawler
     * @param crawlerType type of Crawler
     * @return future to get know if obtaining was successful
     * @throws CradleStorageException if given parameters are invalid
     */
    CompletableFuture<Iterable<Interval>> getIntervalsPerDayAsync(Instant from, Instant to, String crawlerName, String crawlerVersion, String crawlerType) throws CradleStorageException;

    /**
     * Obtains iterable of intervals with startTime greater than or equal to "from" and less than or equal to "to"
     * @param from time from which intervals are searched
     * @param to time to which intervals are searched
     * @param crawlerName name of Crawler
     * @param crawlerVersion version of Crawler
     * @param crawlerType type of Crawler
     * @return iterable of intervals
     * @throws IOException if data retrieval failed
     */
    Iterable<Interval> getIntervals(Instant from, Instant to, String crawlerName, String crawlerVersion, String crawlerType) throws IOException;

    /**
     * Sets last update time and last update date of interval.
     * @param interval interval in which last update time and date will be set
     * @param newLastUpdateTime new time of last update
     * @return the new instance of {@link Interval} with updated newLastUpdateTime. This operation is successful
     * only if lastUpdateTime and lastUpdateDate parameters are the same as previousLastUpdateTime and previousLastUpdateDate.
     * If it was not successful throws an {@link com.exactpro.cradle.utils.UpdateNotAppliedException} exception
     * @throws IOException if data update failed and not UpdateNotAppliedException was thrown
     */
    Interval setIntervalLastUpdateTimeAndDate(Interval interval, Instant newLastUpdateTime) throws IOException;

    /**
     * Asynchronously sets last update time and last update date of interval.
     * @param interval interval in which last update time and date will be set
     * @param newLastUpdateTime new time of last update
     * @return CompletableFuture with the new instance of {@link Interval} with updated newLastUpdateTime. This operation is successful
     * only if lastUpdateTime and lastUpdateDate parameters are the same as previousLastUpdateTime and previousLastUpdateDate.
     * If it was not successful throws an {@link java.util.concurrent.ExecutionException} with cause {@link com.exactpro.cradle.utils.UpdateNotAppliedException} exception
     */
    CompletableFuture<Interval> setIntervalLastUpdateTimeAndDateAsync(Interval interval, Instant newLastUpdateTime);

    /**
     * Updates RecoveryState, also sets lastUpdateTime and lastUpdateDate as current time and date
     * @param interval interval in which Recovery State will be updated
     * @param recoveryState information for recovering of Crawler
     * @return the new instance of {@link Interval} with updated recoveryState. This operation is successful
     * only if lastUpdateTime and lastUpdateDate parameters are the same as previousLastUpdateTime and previousLastUpdateDate.
     * If it was not successful throws an {@link com.exactpro.cradle.utils.UpdateNotAppliedException} exception
     * @throws IOException if data update failed and not UpdateNotAppliedException was thrown
     */
    Interval updateRecoveryState(Interval interval, String recoveryState) throws IOException;

    /**
     * Asynchronously updates RecoveryState, also sets lastUpdateTime and lastUpdateDate as current time and date
     * @param interval interval in which Recovery State will be updated
     * @param recoveryState information for recovering of Crawler
     * @return CompletableFuture with the new instance of {@link Interval} with updated recoveryState. This operation is successful
     * only if lastUpdateTime and lastUpdateDate parameters are the same as previousLastUpdateTime and previousLastUpdateDate.
     * If it was not successful throws an {@link java.util.concurrent.ExecutionException} with cause {@link com.exactpro.cradle.utils.UpdateNotAppliedException} exception
     */
    CompletableFuture<Interval> updateRecoveryStateAsync(Interval interval, String recoveryState);

    /**
     * Sets flag that indicates if interval was processed completely, also sets lastUpdateTime and lastUpdateDate as current time and date
     * @param interval interval in which flag will be set
     * @param processed whether interval was processed completely
     * @return the new instance of {@link Interval} with updated processed. This operation is successful
     * only if lastUpdateTime and lastUpdateDate parameters are the same as previousLastUpdateTime and previousLastUpdateDate.
     * If it was not successful throws an {@link com.exactpro.cradle.utils.UpdateNotAppliedException} exception
     * @throws IOException if settomg data failed and not UpdateNotAppliedException was thrown
     */
    Interval setIntervalProcessed(Interval interval, boolean processed) throws IOException;

    /**
     * Asynchronously sets flag that indicates if interval was processed completely, also sets lastUpdateTime and lastUpdateDate as current time and date
     * @param interval interval in which flag will be set
     * @param processed whether interval was processed completely
     * @return CompletableFuture with the new instance of {@link Interval} with updated processed. This operation is successful
     * only if lastUpdateTime and lastUpdateDate parameters are the same as previousLastUpdateTime and previousLastUpdateDate.
     * If it was not successful throws an {@link java.util.concurrent.ExecutionException} with cause {@link com.exactpro.cradle.utils.UpdateNotAppliedException} exception
     */
    CompletableFuture<Interval> setIntervalProcessedAsync(Interval interval, boolean processed);
}

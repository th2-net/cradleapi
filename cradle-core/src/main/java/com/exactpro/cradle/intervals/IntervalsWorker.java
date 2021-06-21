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
     * Obtains iterable of intervals with startTime greater or equal that from and less or equal then to
     * @param from time from which intervals are being searched
     * @return iterable of intervals
     */
    Iterable<Interval> getIntervals(Instant from, Instant to, String crawlerName, String crawlerVersion) throws IOException;

    /**
     * Asynchronously obtains iterable of intervals with startTime greater or equal that from and less or equal then to
     * @param from time from which intervals are being searched
     * @return future to get know if obtaining was successful
     */
    CompletableFuture<Iterable<Interval>> getIntervalsAsync(Instant from, Instant to, String crawlerName, String crawlerVersion);

    /**
     * Sets last update time and last update date of interval.
     * @param interval interval in which last update time and date will be set
     * @param newLastUpdateTime new time of last update
     * @return true if time and date of last update was set successfully, false otherwise. This operation is successful
     * only if lastUpdateTime and lastUpdateDate parameters are the same as previousLastUpdateTime and previousLastUpdateDate
     */
    boolean setIntervalLastUpdateTimeAndDate(Interval interval, Instant newLastUpdateTime) throws IOException;

    /**
     * Asynchronously sets last update time and last update date of interval.
     * @param interval interval in which last update time and date will be set
     * @param newLastUpdateTime new time of last update
     * @return CompletableFuture with true inside if time and date of last update was set successfully, false otherwise.
     * This operation is successful only if lastUpdateTime and lastUpdateDate parameters are the same as previousLastUpdateTime and previousLastUpdateDate
     */
    CompletableFuture<Boolean> setIntervalLastUpdateTimeAndDateAsync(Interval interval, Instant newLastUpdateTime);

    /**
     * Updates RecoveryState, also sets lastUpdateTime and lastUpdateDate as current time and date
     * @param interval interval in which Recovery State will be updated
     * @param recoveryState information for recovering of Crawler
     * @return true if recovery state was updated successfully, false otherwise
     */
    boolean updateRecoveryState(Interval interval, RecoveryState recoveryState) throws IOException;

    /**
     * Asynchronously updates RecoveryState, also sets lastUpdateTime and lastUpdateDate as current time and date
     * @param interval interval in which Recovery State will be updated
     * @param recoveryState information for recovering of Crawler
     * @return CompletableFuture with true inside if recovery state was updated successfully, false otherwise
     */
    CompletableFuture<Boolean> updateRecoveryStateAsync(Interval interval, RecoveryState recoveryState);

    /**
     * Sets flag that indicates if interval was processed completely, also sets lastUpdateTime and lastUpdateDate as current time and date
     * @param interval interval in which flag will be set
     * @param processed whether interval was processed completely
     * @return true if flag was updated successfully, false otherwise
     */
    boolean setIntervalProcessed(Interval interval, boolean processed) throws IOException;

    /**
     * Asynchronously sets flag that indicates if interval was processed completely, also sets lastUpdateTime and lastUpdateDate as current time and date
     * @param interval interval in which flag will be set
     * @param processed whether interval was processed completely
     * @return CompletableFuture with true inside if flag was updated successfully, false otherwise
     */
    CompletableFuture<Boolean> setIntervalProcessedAsync(Interval interval, boolean processed);
}

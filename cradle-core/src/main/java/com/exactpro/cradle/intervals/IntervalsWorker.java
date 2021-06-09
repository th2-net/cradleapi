package com.exactpro.cradle.intervals;

import java.io.IOException;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;

public interface IntervalsWorker
{
    /**
     * Writes to storage interval
     * @param interval data to write
     * @throws IOException if data writing failed
     */
    void storeInterval(Interval interval) throws IOException;

    /**
     * Asynchronously writes to storage interval
     * @param interval stored interval
     * @return future to get know if storing was successful
     */
    CompletableFuture<Void> storeIntervalAsync(Interval interval);

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

    void updateRecoveryState(Interval interval, RecoveryState recoveryState) throws IOException;

    CompletableFuture<Void> updateRecoveryStateAsync(Interval interval, RecoveryState recoveryState);
}

package com.exactpro.cradle.cassandra.utils;

import org.assertj.core.api.Assertions;
import org.testng.annotations.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;

public class FutureTrackerTest {
    private final long NO_PAUSE_MILLIS = 75;
    private final long DELAY_MILLIS = 100;

    private CompletableFuture<Integer> getFutureWithException () {
        return CompletableFuture.supplyAsync(() -> {
            throw new RuntimeException();
        });
    }

    private CompletableFuture<Integer> getFutureWithDelay () {
        return CompletableFuture.supplyAsync(() -> {
            try {
                var before = Instant.now();
                Thread.sleep(DELAY_MILLIS);
            } catch (InterruptedException e) {
                throw new RuntimeException();
            }

            return 0;
        });
    }

    private CompletableFuture<Integer> chainFuture (CompletableFuture<Integer> future) {
        return future.thenApplyAsync((res) -> {
            try {
                var before = Instant.now();
                Thread.sleep(DELAY_MILLIS);
            } catch (InterruptedException e) {
                throw new RuntimeException();
            }

            return res;
        });
    }

    @Test
    public void testEmptyTracker () {
        Instant start = Instant.now();
        FutureTracker futureTracker = new FutureTracker();
        futureTracker.awaitRemaining();

        Assertions.assertThat(Duration.between(start, Instant.now())).isLessThan(Duration.ofMillis(NO_PAUSE_MILLIS));
    }

    @Test
    public void testTrackingSingleFuture () {
        long expectedTrackingTime = DELAY_MILLIS;
        Instant start = Instant.now();
        FutureTracker futureTracker = new FutureTracker();

        futureTracker.trackFuture(getFutureWithDelay());

        futureTracker.awaitRemaining();

        Assertions.assertThat(Duration.between(start, Instant.now())).isGreaterThan(Duration.ofMillis(expectedTrackingTime));
    }

    @Test
    public void testTrackingFutureWithException () {
        Instant start = Instant.now();
        FutureTracker futureTracker = new FutureTracker();

        futureTracker.trackFuture(getFutureWithException());

        futureTracker.awaitRemaining();

        Assertions.assertThat(Duration.between(start, Instant.now())).isLessThan(Duration.ofMillis(NO_PAUSE_MILLIS));
    }

    @Test
    public void testTracking5Futures() {
        long expectedTrackingTime = 5* DELAY_MILLIS;
        Instant start = Instant.now();
        FutureTracker futureTracker = new FutureTracker();

        CompletableFuture<Integer> lastFuture = null;
        for (int i = 0; i < 5; i ++) {
            CompletableFuture<Integer> curFuture = getFutureWithDelay();
            if (i != 0) {
                curFuture = chainFuture(lastFuture);
            }
            futureTracker.trackFuture(curFuture);
            lastFuture = curFuture;
        }

        futureTracker.awaitRemaining();

        Assertions.assertThat(Duration.between(start, Instant.now())).isGreaterThan(Duration.ofMillis(expectedTrackingTime));
    }
}

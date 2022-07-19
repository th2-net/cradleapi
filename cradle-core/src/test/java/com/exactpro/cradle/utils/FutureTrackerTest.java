package com.exactpro.cradle.utils;

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
        FutureTracker<Integer> futureTracker = new FutureTracker<>();
        futureTracker.awaitRemaining();

        Assertions.assertThat(futureTracker.isEmpty()).isTrue();
        Assertions.assertThat(Duration.between(start, Instant.now())).isLessThan(Duration.ofMillis(NO_PAUSE_MILLIS));
    }

    @Test
    public void testTrackingSingleFuture () {
        long expectedTrackingTime = DELAY_MILLIS;
        Instant start = Instant.now();
        FutureTracker<Integer> futureTracker = new FutureTracker<>();

        futureTracker.track(getFutureWithDelay());
        Assertions.assertThat(futureTracker.isEmpty()).isFalse();

        futureTracker.awaitRemaining();

        Assertions.assertThat(Duration.between(start, Instant.now())).isGreaterThan(Duration.ofMillis(expectedTrackingTime));
    }

    @Test
    public void testTrackingFutureWithException () {
        Instant start = Instant.now();
        FutureTracker<Integer> futureTracker = new FutureTracker<>();

        futureTracker.track(getFutureWithException());

        futureTracker.awaitRemaining();

        Assertions.assertThat(Duration.between(start, Instant.now())).isLessThan(Duration.ofMillis(NO_PAUSE_MILLIS));
    }

    @Test
    public void testTracking5Futures() {
        long expectedTrackingTime = 5 * DELAY_MILLIS;
        Instant start = Instant.now();
        FutureTracker<Integer> futureTracker = new FutureTracker<>();

        CompletableFuture<Integer> lastFuture = null;
        for (int i = 0; i < 5; i ++) {
            CompletableFuture<Integer> curFuture = getFutureWithDelay();
            if (i != 0) {
                curFuture = chainFuture(lastFuture);
            }
            futureTracker.track(curFuture);
            lastFuture = curFuture;
        }
        Assertions.assertThat(futureTracker.isEmpty()).isFalse();
        Assertions.assertThat(futureTracker.remaining()).isEqualTo(5);

        futureTracker.awaitRemaining();

        Assertions.assertThat(Duration.between(start, Instant.now())).isGreaterThan(Duration.ofMillis(expectedTrackingTime));
    }

    @Test
    public void testAwaitZeroTimeout() {
        Instant start = Instant.now();
        FutureTracker<Integer> futureTracker = new FutureTracker<>();

        futureTracker.track(getFutureWithDelay());

        futureTracker.awaitRemaining(0);

        Assertions.assertThat(Duration.between(start, Instant.now())).isLessThan(Duration.ofMillis(NO_PAUSE_MILLIS));

    }
}

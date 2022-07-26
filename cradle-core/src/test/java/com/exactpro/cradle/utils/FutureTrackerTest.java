package com.exactpro.cradle.utils;

import org.assertj.core.api.Assertions;
import org.testng.annotations.Test;

import java.util.concurrent.CompletableFuture;

public class FutureTrackerTest {
    private final long NO_DELAY_MILLIS = 75;
    private final long DELAY_MILLIS = 100;
    private final long PAUSE_MILLIS = 20;
    private static final long WAIT_TIMEOUT_MILLIS = 3_000;

    /*
        Following Runnable will be applied to FutureTracker
        asynchronously
     */
    private static class WaitingRunnable implements Runnable {
        private final long sleepMillis;
        private boolean ready;

        public WaitingRunnable(long sleepMillis) {
            this.sleepMillis = sleepMillis;
            this.ready = false;
        }

        private boolean isReady () {
            return ready;
        }

        @Override
        public void run() {
            try {
                this.ready = true;
                synchronized (this) {
                    this.wait(WAIT_TIMEOUT_MILLIS);
                }

                Thread.sleep(sleepMillis);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private CompletableFuture<Integer> getFutureWithException () {
        return CompletableFuture.supplyAsync(() -> {
            throw new RuntimeException();
        });
    }

    private CompletableFuture<Integer> getFutureWithDelay (WaitingRunnable waitingRunnable) {
        return CompletableFuture.supplyAsync(() -> {
            waitingRunnable.run();
            return 0;
        });
    }

    private CompletableFuture<Integer> chainFuture (CompletableFuture<Integer> future) {
        return future.thenApply((res) -> {
            try {
                Thread.sleep(DELAY_MILLIS);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            return res;
        });
    }

    private void waitForReady (WaitingRunnable waitingRunnable) {
        // Make sure async task is started before we start timer
        while (!waitingRunnable.isReady()) {
            try {
                Thread.sleep(PAUSE_MILLIS);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        synchronized (waitingRunnable) {
            waitingRunnable.notifyAll();
        }
    }

    @Test
    public void testEmptyTracker () {
        long expectedTrackingMillis = NO_DELAY_MILLIS;

        FutureTracker<Integer> futureTracker = new FutureTracker<>();
        long start = System.nanoTime();
        futureTracker.awaitRemaining();

        Assertions.assertThat(futureTracker.isEmpty()).isTrue();

        long actualTrackingMillis = (System.nanoTime() - start)/1_000_000;
        Assertions.assertThat(actualTrackingMillis).isLessThan(expectedTrackingMillis);
        Assertions.assertThat(actualTrackingMillis).isLessThan(WAIT_TIMEOUT_MILLIS);
    }

    @Test
    public void testTrackingSingleFuture () {
        long expectedTrackingMillis = DELAY_MILLIS;

        FutureTracker<Integer> futureTracker = new FutureTracker<>();

        WaitingRunnable waitingRunnable = new WaitingRunnable(DELAY_MILLIS);

        futureTracker.track(getFutureWithDelay(waitingRunnable));
        Assertions.assertThat(futureTracker.isEmpty()).isFalse();

        waitForReady(waitingRunnable);

        long start = System.nanoTime();
        futureTracker.awaitRemaining();

        long actualTrackingMillis = (System.nanoTime() - start)/1_000_000;
        Assertions.assertThat(actualTrackingMillis).isGreaterThanOrEqualTo(expectedTrackingMillis);
        Assertions.assertThat(actualTrackingMillis).isLessThan(WAIT_TIMEOUT_MILLIS);
    }

    @Test
    public void testTrackingFutureWithException () {
        long expectedTrackingMillis = NO_DELAY_MILLIS;

        FutureTracker<Integer> futureTracker = new FutureTracker<>();

        long start = System.nanoTime();
        futureTracker.track(getFutureWithException());
        futureTracker.awaitRemaining();

        long actualTrackingMillis = (System.nanoTime() - start)/1_000_000;
        Assertions.assertThat(actualTrackingMillis).isLessThan(expectedTrackingMillis);
        Assertions.assertThat(actualTrackingMillis).isLessThan(WAIT_TIMEOUT_MILLIS);
    }

    @Test
    public void testTracking5Futures() {
        long expectedTrackingTime = 5 * DELAY_MILLIS;

        WaitingRunnable waitingRunnable = new WaitingRunnable(DELAY_MILLIS);

        FutureTracker<Integer> futureTracker = new FutureTracker<>();

        CompletableFuture<Integer> lastFuture = null;
        for (int i = 0; i < 5; i ++) {
            CompletableFuture<Integer> curFuture = getFutureWithDelay(waitingRunnable);
            if (i != 0) {
                curFuture = chainFuture(lastFuture);
            }
            futureTracker.track(curFuture);
            lastFuture = curFuture;
        }
        Assertions.assertThat(futureTracker.isEmpty()).isFalse();
        Assertions.assertThat(futureTracker.remaining()).isEqualTo(5);

        waitForReady(waitingRunnable);

        long start = System.nanoTime();
        futureTracker.awaitRemaining();

        long actualTrackingMillis = (System.nanoTime() - start)/1_000_000;
        Assertions.assertThat(actualTrackingMillis).isGreaterThanOrEqualTo(expectedTrackingTime);
        Assertions.assertThat(actualTrackingMillis).isLessThan(WAIT_TIMEOUT_MILLIS);
    }

    @Test
    public void testAwaitZeroTimeout() {
        long expectedTrackingMillis = NO_DELAY_MILLIS;

        WaitingRunnable waitingRunnable = new WaitingRunnable(DELAY_MILLIS);

        FutureTracker<Integer> futureTracker = new FutureTracker<>();
        futureTracker.track(getFutureWithDelay(waitingRunnable));

        long start = System.nanoTime();
        futureTracker.awaitRemaining(0);


        long actualTrackingMillis = (System.nanoTime() - start)/1_000_000;
        Assertions.assertThat(actualTrackingMillis).isLessThan(expectedTrackingMillis);
        Assertions.assertThat(actualTrackingMillis).isLessThan(WAIT_TIMEOUT_MILLIS);
    }
}

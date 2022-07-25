package com.exactpro.cradle.utils;

import org.assertj.core.api.Assertions;
import org.testng.annotations.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.ReentrantLock;

public class FutureTrackerTest {
    private final long NO_DELAY_MILLIS = 75;
    private final long DELAY_MILLIS = 100;

    /*
        Following Runnable will be applied to FutureTracker
        asynchronously
     */
    private static class LockedRunnable implements Runnable {

        private final long sleepMillis;
        private final ReentrantLock lock;

        public LockedRunnable(long sleepMillis, ReentrantLock lock) {
            this.sleepMillis = sleepMillis;
            this.lock = lock;
        }

        @Override
        public void run() {
            try {
                lock.lock();
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

    private CompletableFuture<Integer> getFutureWithDelay (ReentrantLock lock) {
        return CompletableFuture.supplyAsync(() -> {
            new LockedRunnable(DELAY_MILLIS, lock).run();
            lock.unlock();
            return 0;
        });
    }

    private CompletableFuture<Integer> chainFuture (CompletableFuture<Integer> future, ReentrantLock lock) {
        return future.thenApplyAsync((res) -> {
            new LockedRunnable(DELAY_MILLIS, lock).run();
            lock.unlock();

            return res;
        });
    }

    @Test
    public void testEmptyTracker () {
        long expectedTrackingMillis = NO_DELAY_MILLIS;

        FutureTracker<Integer> futureTracker = new FutureTracker<>();
        long start = System.nanoTime();
        futureTracker.awaitRemaining();

        Assertions.assertThat(futureTracker.isEmpty()).isTrue();

        long actualTrackingMillis = (System.nanoTime() - start)/1000_000;
        Assertions.assertThat(actualTrackingMillis).isLessThan(expectedTrackingMillis);
    }

    @Test
    public void testTrackingSingleFuture () {
        long expectedTrackingMillis = DELAY_MILLIS;

        FutureTracker<Integer> futureTracker = new FutureTracker<>();

        ReentrantLock lock = new ReentrantLock();
        lock.lock();

        futureTracker.track(getFutureWithDelay(lock));
        Assertions.assertThat(futureTracker.isEmpty()).isFalse();

        lock.unlock();
        long start = System.nanoTime();
        futureTracker.awaitRemaining();

        long actualTrackingMillis = (System.nanoTime() - start)/1000_000;
        Assertions.assertThat(actualTrackingMillis).isGreaterThanOrEqualTo(expectedTrackingMillis);
    }

    @Test
    public void testTrackingFutureWithException () {
        long expectedTrackingMillis = NO_DELAY_MILLIS;

        FutureTracker<Integer> futureTracker = new FutureTracker<>();

        long start = System.nanoTime();
        futureTracker.track(getFutureWithException());
        futureTracker.awaitRemaining();

        long actualTrackingMillis = (System.nanoTime() - start)/1000_000;
        Assertions.assertThat(actualTrackingMillis).isLessThan(expectedTrackingMillis);
    }

    @Test
    public void testTracking5Futures() {
        long expectedTrackingTime = 5 * DELAY_MILLIS;

        ReentrantLock lock = new ReentrantLock();
        lock.lock();

        FutureTracker<Integer> futureTracker = new FutureTracker<>();

        CompletableFuture<Integer> lastFuture = null;
        for (int i = 0; i < 5; i ++) {
            CompletableFuture<Integer> curFuture = getFutureWithDelay(lock);
            if (i != 0) {
                curFuture = chainFuture(lastFuture, lock);
            }
            futureTracker.track(curFuture);
            lastFuture = curFuture;
        }
        Assertions.assertThat(futureTracker.isEmpty()).isFalse();
        Assertions.assertThat(futureTracker.remaining()).isEqualTo(5);

        lock.unlock();
        long start = System.nanoTime();
        futureTracker.awaitRemaining();

        long actualTrackingMillis = (System.nanoTime() - start)/1000_000;
        Assertions.assertThat(actualTrackingMillis).isGreaterThanOrEqualTo(expectedTrackingTime);
    }

    @Test
    public void testAwaitZeroTimeout() {
        long expectedTrackingMillis = NO_DELAY_MILLIS;

        ReentrantLock lock = new ReentrantLock();
        lock.lock();

        FutureTracker<Integer> futureTracker = new FutureTracker<>();
        futureTracker.track(getFutureWithDelay(lock));

        lock.unlock();
        long start = System.nanoTime();
        futureTracker.awaitRemaining(0);


        long actualTrackingMillis = (System.nanoTime() - start)/1000_000;
        Assertions.assertThat(actualTrackingMillis).isLessThan(expectedTrackingMillis);
    }
}

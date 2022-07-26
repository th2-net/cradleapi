package com.exactpro.cradle.utils;

/*
    Following Runnable will be applied to FutureTracker
    asynchronously
 */
public class WaitingRunnable implements Runnable {
    public static final long WAIT_TIMEOUT_MILLIS = 3_000;

    private final long sleepMillis;
    private boolean ready;

    public WaitingRunnable(long sleepMillis) {
        this.sleepMillis = sleepMillis;
        this.ready = false;
    }

    public boolean isReady() {
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

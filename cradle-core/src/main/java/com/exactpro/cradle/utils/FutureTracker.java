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
package com.exactpro.cradle.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Following class tracks futures and when needed tries to wait for them
 */
public class FutureTracker<T> {
    private static final Logger logger = LoggerFactory.getLogger(FutureTracker.class);

    private final Set<CompletableFuture<T>> futures;
    private volatile boolean enabled;

    public FutureTracker () {
        this.futures = new HashSet<>();
        this.enabled = true;
    }

    /**
     * Track a future
     * @param future to be tracked
     */
    public void track(CompletableFuture<T> future) {
        if (enabled) {
            if (future.isDone()) {
                return;
            }
            synchronized (futures) {
                futures.add(future);
            }
            future.whenComplete((res, ex) -> {
                synchronized (futures) {
                    futures.remove(future);
                }
            });
        }
    }

    /**
     * Stops accepting new futures and waits for
     * currently tracked futures
     */
    public void awaitRemaining () {
        awaitRemaining(-1);
    }

    /**
     * Waits for tracked futures, cancels futures after timeout.
     * Cancels all futures without wait if passed 0.
     * Waits without timeout if passed negative.
     * @param timeoutMillis milliseconds to wait
     */
    public void awaitRemaining (long timeoutMillis) {
        long timeoutNanos = timeoutMillis * 1000;
        long startNanos = System.nanoTime();
        this.enabled = false;

        List<CompletableFuture<T>> remainingFutures;
        synchronized (futures) {
            if (futures.isEmpty()) {
                logger.debug("Future Tracker does not have any active futures, finishing tracking without await");
            } else {
                logger.debug("Future Tracker has {} active futures, waiting for them to finish", futures.size());
            }

            remainingFutures = new ArrayList<>(futures);
        }

        for (CompletableFuture<T> future : remainingFutures) {
            long curNanos = System.nanoTime();

            try {
                if (!future.isDone()) {
                    if (timeoutMillis < 0) {
                        future.get();
                    } else {
                        if (startNanos + timeoutNanos < curNanos) {
                            future.cancel(true);
                        } else {
                            future.get(timeoutMillis - (curNanos - startNanos), TimeUnit.NANOSECONDS);
                        }
                    }
                }
            } catch (InterruptedException e) {
                logger.error("Interrupt was called while awaiting futures {}", e.toString());
                throw new RuntimeException(e);
            } catch (ExecutionException e) {
                logger.warn("Exception was thrown during execution {}", e.toString());
            } catch (TimeoutException e) {
                logger.warn("Await timeout was exceeded {}", e.toString());
            }
        }
    }

    /**
     * Informational method for getting remaining number of futures
     * @return number of unfinished futures
     */
    public int remaining () {
        synchronized (futures) {
            return futures.size();
        }
    }

    /**
     * Informs if there are any active futures
     * @return true if there are no active futures
     */
    public boolean isEmpty () {
        return remaining() == 0;
    }
}

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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

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

    public void trackFuture (CompletableFuture<T> future) {
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

    public void awaitRemaining () {
        this.enabled = false;

        List<CompletableFuture<T>> ls;
        synchronized (futures) {
            if (futures.isEmpty()) {
                logger.info("Future Tracker does not have any active futures, finishing tracking without await");
            } else {
                logger.info("Future Tracker has {} active futures, waiting for them to finish", futures.size());
            }

            ls = new ArrayList<>(futures);
        }

        for (CompletableFuture<T> el : ls) {
            try {
                if (!el.isDone()) {
                    el.get();
                }
            } catch (InterruptedException e) {
                logger.error("Interrupt was called while awaiting futures {}", e.getMessage());
                throw new RuntimeException(e);
            } catch (ExecutionException e) {
                logger.warn("Exception was thrown during execution {}", e.getMessage());
            }
        }
    }
}

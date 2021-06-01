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

package com.exactpro.cradle.cassandra.dao.retry;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.oss.driver.api.mapper.MapperContext;

public abstract class AbstractRetryDao {

    public static final String MIN_TIMEOUT_KEY = "minTimeout";
    public static final String MAX_TIMEOUT_KEY = "maxTimeout";
    public static final String COUNT_ATTEMPTS_KEY = "countAttempts";

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractRetryDao.class);

    private final long minTimeout;
    private final long countAttempts;
    private final long h;

    public AbstractRetryDao(MapperContext context) {
        Map<Object, Object> customState = context.getCustomState();
        long maxTimeout;

        if (customState.get(MIN_TIMEOUT_KEY) != null) {
            minTimeout = ((Number) customState.get(MIN_TIMEOUT_KEY)).longValue();
        } else {
            minTimeout = 1000L;
        }

        if (customState.get(MAX_TIMEOUT_KEY) != null) {
            maxTimeout = ((Number) customState.get(MAX_TIMEOUT_KEY)).longValue();
        } else {
            maxTimeout = 60000L;
        }

        if (customState.get(COUNT_ATTEMPTS_KEY) != null) {
            countAttempts = ((Number) customState.get(COUNT_ATTEMPTS_KEY)).longValue();
        } else {
            countAttempts = 5L;
        }

        h = (maxTimeout - minTimeout) / countAttempts;
    }

    protected <T> T blockingRequest(String methodName, Supplier<T> supplier) {
        try {
            return supplier.get();
        } catch (Exception e) {
            LOGGER.warn("Cannot execute blocking request for method {}. Exception message: {}", methodName, e.getMessage());

            for (int i = 0; i < countAttempts; i++) {
                try {
                    Thread.sleep(getTimeout(i));
                } catch (InterruptedException ex) {
                    throw e;
                }

                try {
                    return supplier.get();
                } catch (Exception ex) {
                    e.addSuppressed(ex);
                    LOGGER.warn("Cannot retry blocking request for method with name {}. Count attempt = {}. Timeout = {}", methodName, i + 1, getTimeout(i));
                }
            }

            LOGGER.warn("Cannot retry blocking request for method with name {}", methodName);
            throw e;
        }
    }

    protected <T> CompletableFuture<T> asyncRequest(String methodName, Supplier<CompletableFuture<T>> func) {
        return asyncRequest(methodName, func, new AtomicLong());
    }

    private <T> CompletableFuture<T> asyncRequest(String methodName, Supplier<CompletableFuture<T>> func, AtomicLong attempts) {
        return func.get().handle((result, throwable) -> {
            if (throwable == null) {
                return result;
            }

            if (attempts.get() > countAttempts) {
                if (throwable instanceof CompletionException) {
                    throw (CompletionException) throwable;
                } else {
                    throw new CompletionException(throwable);
                }
            }

            long localAttempt = attempts.getAndIncrement();
            LOGGER.warn("Can not execute async request for method with name {}. Count attempt = {}. Timeout = {}", methodName, localAttempt, getTimeout(localAttempt));
            try {
                Thread.sleep(getTimeout(localAttempt));
                return asyncRequest(methodName, func, attempts).get();
            } catch (InterruptedException | ExecutionException e) {
                throw new CompletionException(e);
            }
        });
    }

    private long getTimeout(long count) {
        return  minTimeout + h * count;
    }
}

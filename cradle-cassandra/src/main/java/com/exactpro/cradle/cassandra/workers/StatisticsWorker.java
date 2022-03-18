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
package com.exactpro.cradle.cassandra.workers;

import com.exactpro.cradle.Counter;
import com.exactpro.cradle.EntityType;
import com.exactpro.cradle.FrameType;
import com.exactpro.cradle.cassandra.SerializedEntityMetadata;
import com.exactpro.cradle.cassandra.counters.CounterCache;
import com.exactpro.cradle.cassandra.counters.TimeFrameCounter;
import com.exactpro.cradle.cassandra.dao.CradleOperators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class StatisticsWorker implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(StatisticsWorker.class);

    private final Map<EntityType, CounterCache> entityCounters;
    private final CradleOperators ops;
    private final long interval;

    public StatisticsWorker(CradleOperators ops, long persistanceInterval) {
        this.ops = ops;
        this.interval = persistanceInterval;

        entityCounters = new EnumMap<>(EntityType.class);
        for (EntityType t : EntityType.values())
            entityCounters.put(t, new CounterCache());
    }


    private ScheduledExecutorService executorService;
    public void start() {
        if (interval == 0) {
            logger.info("Counter persistence service is disabled");
            return;
        }

        logger.info("Starting executor for StatisticsWorker");
        executorService = Executors.newScheduledThreadPool(1);
        executorService.scheduleAtFixedRate(this, 0, interval, TimeUnit.MILLISECONDS);
        logger.info("StatisticsWorker executor started");
    }

    public void stop() {
        if (interval == 0)
            return;

        if (executorService == null)
            throw new IllegalStateException("Can not stop statistics worker as it is not started");
        logger.info("Shutting down StatisticsWorker executor");
        executorService.shutdown();
        try {
            logger.info("Waiting StatisticsWorker jobs to complete");
            executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
            logger.info("StatisticsWorker shutdown complete");
        } catch (InterruptedException e) {
            logger.error("Interrupted while waiting jobs to complete");
        }
    }


    public void addEntityBatchStatistics(EntityType entityType, Collection<SerializedEntityMetadata> batch) {

        CounterCache counters = entityCounters.get(entityType);
        batch.forEach(meta -> {
            Counter counter = new Counter(1, meta.getSerializedEntitySize());
            for (FrameType t : FrameType.values()) {
                counters.getCounterSamples(t).update(meta.getTimestamp(), counter);
            }
        });
    }

    private void persistCounter(EntityType entityType, FrameType frameType, TimeFrameCounter counter) {
        try {
            logger.trace("Persisting counter for %s:%s:%s", entityType, frameType, counter.getFrameStart());
        } catch (Exception e) {
            logger.error("Exception persisting counter, retry scheduled", e);
            entityCounters.get(entityType).getCounterSamples(frameType).update(counter.getFrameStart(), counter.getCounter());
        }
    }

    @Override
    public void run() {
        logger.trace("executing StatisticsWorker job");

        try {
            for (FrameType frameType : FrameType.values()) {
                for (EntityType entityType : EntityType.values()) {
                    Collection<TimeFrameCounter> counters = entityCounters.get(entityType).getCounterSamples(frameType).extractAll();
                    counters.forEach(counter -> persistCounter(entityType, frameType, counter));
                }
            }
        } catch (Exception e) {
            logger.error("Exception processing job", e);
        }

        logger.trace("StatisticsWorker job complete");
    }
}
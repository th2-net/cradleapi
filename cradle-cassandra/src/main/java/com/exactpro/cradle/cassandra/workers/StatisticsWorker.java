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
 */package com.exactpro.cradle.cassandra.workers;

import com.exactpro.cradle.Counter;
import com.exactpro.cradle.EntityType;
import com.exactpro.cradle.FrameType;
import com.exactpro.cradle.cassandra.SerializedEntityMetadata;
import com.exactpro.cradle.cassandra.counters.CounterCache;
import com.exactpro.cradle.cassandra.counters.TimeFrameCounter;
import com.exactpro.cradle.cassandra.dao.CradleOperators;

import java.util.Collection;
import java.util.EnumMap;
import java.util.Map;

public class StatisticsWorker {

    private final Map<EntityType, CounterCache> entityCounters;
    private final CradleOperators ops;

    public StatisticsWorker(CradleOperators ops) {
        this.entityCounters = new EnumMap<>(EntityType.class);
        this.ops = ops;
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

    private void persistCounters() {

        for (FrameType frameType : FrameType.values()) {
            for (EntityType entityType : EntityType.values()) {
                Collection<TimeFrameCounter> counters = entityCounters.get(entityType).getCounterSamples(frameType).extractAll();
            }
        }
    }
}
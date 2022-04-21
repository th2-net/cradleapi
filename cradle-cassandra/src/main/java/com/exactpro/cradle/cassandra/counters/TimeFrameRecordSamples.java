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
package com.exactpro.cradle.cassandra.counters;

import com.exactpro.cradle.FrameType;

import java.time.Instant;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

public class TimeFrameRecordSamples<V> {
    private final FrameType frameType;
    private final Map<Instant, TimeFrameRecord<V>> samples;
    private final TimeFrameRecordFactory<V> recordFactory;

    TimeFrameRecordSamples(FrameType frameType, TimeFrameRecordFactory<V> recordFactory) {
        samples = new HashMap<>();
        this.frameType = frameType;
        this.recordFactory = recordFactory;
    }

    public synchronized Collection<TimeFrameRecord<V>> extractAll() {
        Collection<TimeFrameRecord<V>> result = new LinkedList<>(samples.values());
        samples.clear();
        return result;
    }

    public synchronized void update(Instant time, V record) {
        Instant frameStart = frameType.getFrameStart(time);
        if (samples.containsKey(frameStart)) {
            TimeFrameRecord<V> frameRecord = samples.get(frameStart);
            frameRecord.update(record);
        } else {
            samples.put(frameStart, recordFactory.create(frameStart, record));
        }
    }
}
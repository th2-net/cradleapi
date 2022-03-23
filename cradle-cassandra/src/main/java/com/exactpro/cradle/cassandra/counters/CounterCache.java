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

import java.util.EnumMap;
import java.util.Map;

public class CounterCache {

    private final Map<FrameType, CounterSamples> cache;

    public CounterCache() {
        cache = new EnumMap<>(FrameType.class);
        for (FrameType t: FrameType.values())
            cache.put(t, new CounterSamples(t));
    }


    public CounterSamples getCounterSamples(FrameType type) {
        return cache.get(type);
    }
}
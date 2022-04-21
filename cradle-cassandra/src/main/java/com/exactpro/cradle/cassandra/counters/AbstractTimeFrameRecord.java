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

import java.time.Instant;

public abstract class AbstractTimeFrameRecord<V> implements TimeFrameRecord<V>{

    private final Instant frameStart;
    protected V record;

    protected AbstractTimeFrameRecord(Instant frameStart, V record) {
        this.frameStart = frameStart;
        this.record = record;
    }

    public Instant getFrameStart() {
        return frameStart;
    }

    public synchronized V getRecord() {
        return record;
    }

    public abstract void update(V value);
}
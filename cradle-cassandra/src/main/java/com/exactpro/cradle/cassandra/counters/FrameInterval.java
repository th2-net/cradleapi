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
import com.exactpro.cradle.counters.Interval;

import java.util.Objects;

public class FrameInterval {

    private final FrameType frameType;
    private final Interval interval;

    public FrameInterval(FrameType frameType, Interval interval) {
        this.frameType = frameType;
        this.interval = interval;
    }

    public FrameType getFrameType() {
        return frameType;
    }

    public Interval getInterval() {
        return interval;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FrameInterval frame = (FrameInterval) o;
        return frameType == frame.frameType && interval.equals(frame.interval);
    }

    @Override
    public int hashCode() {
        return Objects.hash(frameType, interval);
    }
}

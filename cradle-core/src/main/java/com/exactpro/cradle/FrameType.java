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
package com.exactpro.cradle;

import java.time.Instant;

public enum FrameType {
    TYPE_100MS(1, 100),
    TYPE_SECOND(2, 1000),
    TYPE_MINUTE(3, 60 * 1000),
    TYPE_HOUR(4, 60 * 60 * 1000);

    private final byte value;
    private final long millisInFrame;
    FrameType(int value, long millisInFrame) {
        this.value = (byte) value;
        this.millisInFrame = millisInFrame;
    }

    public byte getValue() {
        return value;
    }

    /**
     * Calculates start time (inclusive) for the given time
     * @param time for which frame start is calculated
     * @return start time(inclusive) for a given time
     */
    public Instant getFrameStart(Instant time) {
        long millis = time.toEpochMilli();
        long millisAdjusted = (millis / millisInFrame) * millisInFrame;
        return Instant.ofEpochMilli(millisAdjusted);
    }

    /**
     * Calculates end time (exclusive) for the given time
     * @param time for which frame end is calculated
     * @return end time(excluseve) for a given time
     */
    public Instant getFrameEnd(Instant time) {
        return getFrameStart(time.plusMillis(millisInFrame));
    }

    /**
     * Returns FrameType form value
     * @param value
     * @return FrameType that corresponds to given value
     * @throws IllegalArgumentException if value does not match any frame type
     */
    public static FrameType from(int value) {
        for (FrameType e: values())
            if (e.getValue() == value)
                return e;
        throw new IllegalArgumentException(String.format("No frame type associated with value (%d)", value));
    }
}
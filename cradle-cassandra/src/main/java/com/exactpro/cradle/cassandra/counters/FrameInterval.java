package com.exactpro.cradle.cassandra.counters;

import com.exactpro.cradle.FrameType;

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

package com.exactpro.cradle.cassandra.counters;

import com.exactpro.cradle.FrameType;
import com.exactpro.cradle.SessionRecordType;
import com.exactpro.cradle.counters.Interval;

import java.util.Objects;

public class SessionRecordFrameInterval {

    private final FrameType frameType;
    private final Interval interval;
    private final SessionRecordType sessionRecordType;

    public SessionRecordFrameInterval(FrameType frameType, Interval interval, SessionRecordType sessionRecordType) {
        this.frameType = frameType;
        this.interval = interval;
        this.sessionRecordType = sessionRecordType;
    }

    public FrameType getFrameType() {
        return frameType;
    }

    public Interval getInterval() {
        return interval;
    }

    public SessionRecordType getSessionRecordType() {
        return sessionRecordType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SessionRecordFrameInterval that = (SessionRecordFrameInterval) o;
        return frameType == that.frameType && interval.equals(that.interval) && sessionRecordType == that.sessionRecordType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(frameType, interval, sessionRecordType);
    }
}

package com.exactpro.cradle.cassandra.counters;

import java.time.Instant;
import java.util.Objects;

public class Interval {

    private final Instant start, end;

    public Interval (Instant start, Instant end) {
        this.start = start;
        this.end = end;
    }

    public Instant getStart() {
        return start;
    }

    public Instant getEnd() {
        return end;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Interval interval = (Interval) o;
        return start.equals(interval.start) && end.equals(interval.end);
    }

    @Override
    public int hashCode() {
        return Objects.hash(start, end);
    }
}

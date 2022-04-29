package com.exactpro.cradle.cassandra.counters;

import java.time.Instant;

public interface TimeFrameRecord<V> {
    Instant getFrameStart();
    V getRecord();
    void update(V value);
}
package com.exactpro.cradle.cassandra.counters;

import java.time.Instant;

public interface TimeFrameRecordFactory<V> {
    TimeFrameRecord<V> create(Instant frameStart, V record);
}
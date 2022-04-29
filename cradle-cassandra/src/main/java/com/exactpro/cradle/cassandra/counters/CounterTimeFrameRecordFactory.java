package com.exactpro.cradle.cassandra.counters;

import com.exactpro.cradle.counters.Counter;

import java.time.Instant;

public class CounterTimeFrameRecordFactory implements TimeFrameRecordFactory<Counter> {
    @Override
    public CounterTimeFrameRecord create(Instant frameStart, Counter counter) {
        return new CounterTimeFrameRecord(frameStart, counter);
    }
}
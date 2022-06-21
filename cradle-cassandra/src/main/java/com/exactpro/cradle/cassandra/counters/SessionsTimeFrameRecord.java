package com.exactpro.cradle.cassandra.counters;

import java.time.Instant;

public class SessionsTimeFrameRecord extends AbstractTimeFrameRecord<SessionList> {
    SessionsTimeFrameRecord(Instant frameStart, SessionList sessionList) {
        super(frameStart, sessionList);
    }

    @Override
    public synchronized void update(SessionList value) {
        record = record.mergedWith(value);
    }
}
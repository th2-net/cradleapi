package com.exactpro.cradle.cassandra.counters;

import java.time.Instant;

public class SessionsTimeFrameRecordFactory implements TimeFrameRecordFactory<SessionList>{
    @Override
    public TimeFrameRecord<SessionList> create(Instant frameStart, SessionList record) {
        return new SessionsTimeFrameRecord(frameStart, record);
    }
}

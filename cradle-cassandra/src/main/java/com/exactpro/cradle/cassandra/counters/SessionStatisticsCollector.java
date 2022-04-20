package com.exactpro.cradle.cassandra.counters;

import com.exactpro.cradle.BookId;
import com.exactpro.cradle.SessionRecordType;
import com.exactpro.cradle.serialization.SerializedEntityMetadata;

import java.util.Collection;

public interface SessionStatisticsCollector {
    void updateSessionStatistics(BookId bookId, String page, SessionRecordType recordType, String session, Collection<SerializedEntityMetadata> batchMetadata);
}

package com.exactpro.cradle.cassandra.counters;

import com.exactpro.cradle.BookId;
import com.exactpro.cradle.serialization.SerializedEntityMetadata;

import java.util.Collection;

public interface MessageStatisticsCollector {
    void updateMessageBatchStatistics(BookId bookId, String sessionAlias, String direction, Collection<SerializedEntityMetadata> batchMetadata);
}
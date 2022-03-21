package com.exactpro.cradle.cassandra.counters;

import com.exactpro.cradle.BookId;
import com.exactpro.cradle.EntityType;
import com.exactpro.cradle.serialization.SerializedEntityMetadata;

import java.util.Collection;

public interface EntityStatisticsCollector {
    void updateEntityBatchStatistics(BookId bookId, EntityType entityType, Collection<SerializedEntityMetadata> batchMetadata);
}
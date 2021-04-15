package com.exactpro.cradle.cassandra.iterators;

import com.datastax.oss.driver.api.core.MappedAsyncPagingIterable;
import com.exactpro.cradle.cassandra.dao.healing.HealingIntervalEntity;
import com.exactpro.cradle.healing.HealingInterval;

import java.util.Iterator;

public class HealingIntervalsIteratorAdapter implements Iterable<HealingInterval> {
    private final MappedAsyncPagingIterable<HealingIntervalEntity> rows;

    public HealingIntervalsIteratorAdapter(MappedAsyncPagingIterable<HealingIntervalEntity> rows)
    {
        this.rows = rows;
    }

    @Override
    public Iterator<HealingInterval> iterator() {
        return new HealingIntervalsIterator(rows);
    }
}

package com.exactpro.cradle.cassandra.iterators;

import com.datastax.oss.driver.api.core.MappedAsyncPagingIterable;
import com.exactpro.cradle.cassandra.dao.healing.HealingIntervalEntity;
import com.exactpro.cradle.healing.HealingInterval;

import java.io.IOException;

public class HealingIntervalsIterator extends ConvertingPagedIterator<HealingInterval, HealingIntervalEntity>
{

    public HealingIntervalsIterator(MappedAsyncPagingIterable<HealingIntervalEntity> rows) { super(rows); }

    @Override
    protected HealingInterval convertEntity(HealingIntervalEntity entity) throws IOException {
       return entity.asHealingInterval();
    }
}

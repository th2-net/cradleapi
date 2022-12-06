package com.exactpro.cradle.cassandra.dao.intervals.converters;

import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.exactpro.cradle.cassandra.EntityConverter;
import com.exactpro.cradle.cassandra.dao.intervals.IntervalEntity;

@Dao
public interface IntervalEntityConverter extends EntityConverter<IntervalEntity> {
}

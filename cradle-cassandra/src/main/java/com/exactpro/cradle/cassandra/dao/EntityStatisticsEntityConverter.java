package com.exactpro.cradle.cassandra.dao;

import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.exactpro.cradle.cassandra.EntityConverter;

@Dao
public interface EntityStatisticsEntityConverter extends EntityConverter<EntityStatisticsEntity> {
}
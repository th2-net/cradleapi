package com.exactpro.cradle.cassandra.dao.books.converters;

import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.exactpro.cradle.cassandra.EntityConverter;
import com.exactpro.cradle.cassandra.dao.books.PageEntity;

@Dao
public interface PageEntityConverter extends EntityConverter<PageEntity> {
}

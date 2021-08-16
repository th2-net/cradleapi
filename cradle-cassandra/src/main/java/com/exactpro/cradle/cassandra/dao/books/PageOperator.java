package com.exactpro.cradle.cassandra.dao.books;

import java.util.function.Function;

import com.datastax.oss.driver.api.core.PagingIterable;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.annotations.Insert;
import com.datastax.oss.driver.api.mapper.annotations.Select;

@Dao
public interface PageOperator
{
	@Select
	PagingIterable<PageEntity> getAll(Function<BoundStatementBuilder, BoundStatementBuilder> attributes);
	
	@Insert
	PageEntity write(PageEntity entity, Function<BoundStatementBuilder, BoundStatementBuilder> attributes);
}

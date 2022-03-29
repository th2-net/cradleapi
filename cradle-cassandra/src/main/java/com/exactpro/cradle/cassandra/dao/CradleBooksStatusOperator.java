package com.exactpro.cradle.cassandra.dao;

import com.datastax.oss.driver.api.core.PagingIterable;
import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.annotations.Delete;
import com.datastax.oss.driver.api.mapper.annotations.Insert;
import com.datastax.oss.driver.api.mapper.annotations.Select;

@Dao
public interface CradleBooksStatusOperator {

    @Select
    PagingIterable<BooksStatusEntity> getBookStatuses(String bookName);

    @Insert
    void saveBookStatus (BooksStatusEntity entity);

    @Delete(entityClass = BooksStatusEntity.class)
    void deleteBookStatuses (String bookName);
}

package com.exactpro.cradle.cassandra.dao;

import com.datastax.oss.driver.api.mapper.annotations.ClusteringColumn;
import com.datastax.oss.driver.api.mapper.annotations.CqlName;
import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.PartitionKey;

import java.time.Instant;

import static com.exactpro.cradle.cassandra.StorageConstants.*;

@Entity
public class BooksStatusEntity {

    private String bookName;
    private String tableName;
    private Instant created;

    public BooksStatusEntity() {

    }

    public BooksStatusEntity(String bookName, String tableName, Instant created) {
        this.bookName = bookName;
        this.tableName = tableName;
        this.created = created;
    }

    @PartitionKey(0)
    @CqlName(BOOK_NAME)
    public String getBookName() {
        return bookName;
    }

    public void setBookName(String bookName) {
        this.bookName = bookName;
    }

    @ClusteringColumn(1)
    @CqlName(TABLE_NAME)
    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    @CqlName(CREATED)
    public Instant getCreated() {
        return created;
    }

    public void setCreated(Instant created) {
        this.created = created;
    }
}

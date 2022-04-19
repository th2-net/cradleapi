/*
 * Copyright 2020-2022 Exactpro (Exactpro Systems Limited)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.exactpro.cradle.cassandra.dao;

import com.datastax.oss.driver.api.mapper.annotations.ClusteringColumn;
import com.datastax.oss.driver.api.mapper.annotations.CqlName;
import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.PartitionKey;

import java.time.Instant;

@Entity
public class BooksStatusEntity {
    public static final String FIELD_BOOK_NAME = "book_name";
    public static final String FIELD_OBJECT_TYPE = "object_type";
    public static final String FIELD_OBJECT_NAME = "object_name";
    public static final String FIELD_CREATED = "created";
    public static final String FIELD_SCHEMA_VERSION = "schema_version";
    private String bookName;
    private String objectType;
    private String objectName;
    private Instant created;
    private String schemaVersion;

    public BooksStatusEntity() {

    }

    public BooksStatusEntity(String bookName, String objectType, String objectName, Instant created, String schemaVersion) {
        this.bookName = bookName;
        this.objectType = objectType;
        this.objectName = objectName;
        this.created = created;
        this.schemaVersion = schemaVersion;
    }

    @PartitionKey(0)
    @CqlName(FIELD_BOOK_NAME)
    public String getBookName() {
        return bookName;
    }

    public void setBookName(String bookName) {
        this.bookName = bookName;
    }

    @ClusteringColumn(1)
    @CqlName(FIELD_OBJECT_TYPE)
    public String getObjectType() {
        return objectType;
    }

    public void setObjectType(String objectType) {
        this.objectType = objectType;
    }

    @ClusteringColumn(2)
    @CqlName(FIELD_OBJECT_NAME)
    public String getObjectName() {
        return objectName;
    }

    public void setObjectName(String objectName) {
        this.objectName = objectName;
    }

    @CqlName(FIELD_CREATED)
    public Instant getCreated() {
        return created;
    }

    public void setCreated(Instant created) {
        this.created = created;
    }

    @CqlName(FIELD_SCHEMA_VERSION)
    public String getSchemaVersion() {
        return schemaVersion;
    }

    public void setSchemaVersion(String schemaVersion) {
        this.schemaVersion = schemaVersion;
    }
}


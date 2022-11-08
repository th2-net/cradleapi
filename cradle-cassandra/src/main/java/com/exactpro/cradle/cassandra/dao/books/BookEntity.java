/*
 * Copyright 2021-2022 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.cradle.cassandra.dao.books;

import com.datastax.oss.driver.api.mapper.annotations.CqlName;
import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.PartitionKey;
import com.datastax.oss.driver.api.mapper.annotations.PropertyStrategy;

import java.time.Instant;
import java.util.Objects;

/**
 * Contains information about book as stored in "cradle" keyspace
 */
@Entity
@CqlName(BookEntity.TABLE_NAME)
@PropertyStrategy(mutable = false)
public class BookEntity {
	public static final String TABLE_NAME = "books";

    public static final String FIELD_NAME = "name";
    public static final String FIELD_FULLNAME = "fullname";
    public static final String FIELD_DESCRIPTION = "description";
    public static final String FIELD_CREATED = "created";
    public static final String FIELD_SCHEMA_VERSION = "schema_version";

    @PartitionKey(0)
    @CqlName(FIELD_NAME)
    private final String name;

    @CqlName(FIELD_FULLNAME)
    private final String fullName;

    @CqlName(FIELD_DESCRIPTION)
    private final String desc;

    @CqlName(FIELD_CREATED)
    private final Instant created;

    @CqlName(FIELD_SCHEMA_VERSION)
    private final String schemaVersion;

    public BookEntity(String name, String fullName, String desc, Instant created, String schemaVersion) {
        this.name = name;
        this.fullName = fullName;
        this.desc = desc;
        this.created = created;
        this.schemaVersion = schemaVersion;
    }

    public String getName() {
        return name;
    }
    public String getFullName() {
        return fullName;
    }
    public String getDesc() {
        return desc;
    }
    public Instant getCreated() {
        return created;
    }
    public String getSchemaVersion() {
        return schemaVersion;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof BookEntity)) return false;
        BookEntity that = (BookEntity) o;

        return Objects.equals(getName(), that.getName())
                && Objects.equals(getFullName(), that.getFullName())
                && Objects.equals(getDesc(), that.getDesc())
                && Objects.equals(getCreated(), that.getCreated())
                && Objects.equals(getSchemaVersion(), that.getSchemaVersion());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getName(),
                getFullName(),
                getDesc(),
                getCreated(),
                getSchemaVersion());
    }
}

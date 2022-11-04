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

package com.exactpro.cradle.cassandra.dao.labels;

import com.datastax.oss.driver.api.mapper.annotations.*;

import java.util.Objects;

@Entity
@CqlName(LabelEntity.TABLE_NAME)
@PropertyStrategy(mutable = false)
public class LabelEntity {
    public static final String TABLE_NAME = "labels";

    public static final String FIELD_BOOK = "book";
    public static final String FIELD_PAGE = "page";
    public static final String FIELD_NAME = "name";

    @PartitionKey(0)
    @CqlName(FIELD_BOOK)
    private final String book;

    @PartitionKey(1)
    @CqlName(FIELD_PAGE)
    private final String page;

    @ClusteringColumn(1)
    @CqlName(FIELD_NAME)
    private final String name;

    public LabelEntity(String book, String page, String name) {
        this.book = book;
        this.page = page;
        this.name = name;
    }

    public String getBook() {
        return book;
    }
    public String getPage() {
        return page;
    }
    public String getName() {
        return name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof LabelEntity)) return false;
        LabelEntity that = (LabelEntity) o;

        return Objects.equals(getBook(), that.getBook())
                && Objects.equals(getPage(), that.getPage())
                && Objects.equals(getName(), that.getName());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getBook(), getPage(), getName());
    }
}

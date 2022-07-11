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
package com.exactpro.cradle.cassandra.dao.messages;

import com.datastax.oss.driver.api.mapper.annotations.ClusteringColumn;
import com.datastax.oss.driver.api.mapper.annotations.CqlName;
import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.PartitionKey;

@Entity
@CqlName(PageGroupEntity.TABLE_NAME)
public class PageGroupEntity {
    public static final String TABLE_NAME = "page_groups";

    public static final String FIELD_BOOK = "book";
    public static final String FIELD_PAGE = "page";
    public static final String FIELD_GROUP = "group";

    @PartitionKey(0)
    @CqlName(FIELD_BOOK)
    private String book;

    @PartitionKey(1)
    @CqlName(FIELD_PAGE)
    private String page;

    @ClusteringColumn(0)
    @CqlName(FIELD_GROUP)
    private String group;

    public PageGroupEntity () {
    }

    public PageGroupEntity (String book, String page, String group) {
        this.book = book;
        this.page = page;
        this.group = group;
    }

    public String getBook()	{
        return book;
    }

    public void setBook(String book) {
        this.book = book;
    }

    public String getPage() {
        return page;
    }

    public void setPage(String page) {
        this.page = page;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }
}

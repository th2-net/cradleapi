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
package com.exactpro.cradle.cassandra.dao.testevents;

import com.datastax.oss.driver.api.mapper.annotations.*;

@Entity
@PropertyStrategy(mutable = false)
public class EventBatchMaxDurationEntity {


    public static final String TABLE_NAME = "event_batch_max_durations";

    public static final String FIELD_BOOK = "book";
    public static final String FIELD_PAGE = "page";
    public static final String FIELD_SCOPE = "scope";
    public static final String FIELD_MAX_BATCH_DURATION = "max_batch_duration";

    @PartitionKey(1)
    @CqlName(FIELD_BOOK)
    private String book;

    @PartitionKey(2)
    @CqlName(FIELD_PAGE)
    private String page;


    // TODO: should not scope be clustering key?
    @PartitionKey(3)
    @CqlName(FIELD_SCOPE)
    private String scope;

    @CqlName(FIELD_MAX_BATCH_DURATION)
    private long maxBatchDuration;

    public EventBatchMaxDurationEntity(String book, String page, String scope, long maxBatchDuration) {
        this.book = book;
        this.page = page;
        this.scope = scope;
        this.maxBatchDuration = maxBatchDuration;
    }

    public String getBook() {
        return book;
    }

    public String getPage() {
        return page;
    }

    public String getScope() {
        return scope;
    }

    public long getMaxBatchDuration() {
        return maxBatchDuration;
    }
}
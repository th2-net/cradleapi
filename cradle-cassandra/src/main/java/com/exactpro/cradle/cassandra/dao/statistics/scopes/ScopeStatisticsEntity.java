/*
 * Copyright 2023 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.cradle.cassandra.dao.statistics.scopes;

import com.datastax.oss.driver.api.mapper.annotations.*;

import java.time.Instant;
import java.util.Objects;

@Entity
@CqlName(ScopeStatisticsEntity.TABLE_NAME)
@PropertyStrategy(mutable = false)
public class ScopeStatisticsEntity {
    public static final String TABLE_NAME = "scope_statistics";

    public static final String FIELD_BOOK = "book";

    public static final String FIELD_PAGE = "page";

    public static final String FIELD_FRAME_TYPE = "frame_type";
    public static final String FIELD_FRAME_START = "frame_start";
    public static final String FIELD_SCOPE = "scope";

    @PartitionKey(1)
    @CqlName(FIELD_BOOK)
    private final String book;

    @PartitionKey(2)
    @CqlName(FIELD_PAGE)
    private final String page;
    @PartitionKey(3)
    @CqlName(FIELD_FRAME_TYPE)
    private final Byte frameType;

    @ClusteringColumn(1)
    @CqlName(FIELD_FRAME_START)
    private final Instant frameStart;

    @ClusteringColumn(2)
    @CqlName(FIELD_SCOPE)
    private final String scope;

    public ScopeStatisticsEntity(String book, String page, Byte frameType, Instant frameStart, String scope) {
        this.book = book;
        this.page = page;
        this.frameType = frameType;
        this.frameStart = frameStart;
        this.scope = scope;
    }

    public String getBook() {
        return book;
    }
    public String getPage() {
        return page;
    }
    public Byte getFrameType() {
        return frameType;
    }
    public Instant getFrameStart() {
        return frameStart;
    }
    public String getScope() {
        return scope;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        ScopeStatisticsEntity that = (ScopeStatisticsEntity) o;

        return Objects.equals(getBook(), that.getBook()) &&
                Objects.equals(getPage(), that.getPage()) &&
                Objects.equals(getFrameType(), that.getFrameType()) &&
                Objects.equals(getFrameStart(), that.getFrameStart()) &&
                Objects.equals(getScope(), that.getScope());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getBook(),
                getPage(),
                getFrameType(),
                getFrameStart(),
                getScope());
    }

    @Override
    public String toString() {
        return String.format("%s [%s=\"%s\", %s=\"%s\", %s=%d, %s=%s, %s=\"%s\"]"
                , getClass().getSimpleName()
                , FIELD_BOOK, book
                , FIELD_PAGE, page
                , FIELD_FRAME_TYPE, frameType
                , FIELD_FRAME_START, frameStart.toString()
                , FIELD_SCOPE, scope);
    }
}

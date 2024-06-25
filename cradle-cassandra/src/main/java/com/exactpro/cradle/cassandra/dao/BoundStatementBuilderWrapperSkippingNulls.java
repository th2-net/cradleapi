/*
 * Copyright 2024 Exactpro (Exactpro Systems Limited)
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

import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Set;

// This wrapper skips null values to avoid tombstone creation in Cassandra tables
public class BoundStatementBuilderWrapperSkippingNulls {
    private BoundStatementBuilder builder;

    public BoundStatementBuilderWrapperSkippingNulls(PreparedStatement statement) {
        builder = statement.boundStatementBuilder();
    }

    public BoundStatementBuilderWrapperSkippingNulls setString(@NonNull String name, @Nullable String v) {
        if (v != null) {
            builder = builder.setString(name, v);
        }
        return this;
    }

    public BoundStatementBuilderWrapperSkippingNulls setLocalDate(@NonNull String name, @Nullable LocalDate v) {
        if (v != null) {
            builder = builder.setLocalDate(name, v);
        }
        return this;
    }

    public BoundStatementBuilderWrapperSkippingNulls setLocalTime(@NonNull String name, @Nullable LocalTime v) {
        if (v != null) {
            builder = builder.setLocalTime(name, v);
        }
        return this;
    }

    public BoundStatementBuilderWrapperSkippingNulls setBoolean(@NonNull String name, boolean v) {
        builder = builder.setBoolean(name, v);
        return this;
    }

    public BoundStatementBuilderWrapperSkippingNulls setInt(@NonNull String name, int v) {
        builder = builder.setInt(name, v);
        return this;
    }

    public BoundStatementBuilderWrapperSkippingNulls setLong(@NonNull String name, long v) {
        builder = builder.setLong(name, v);
        return this;
    }

    public BoundStatementBuilderWrapperSkippingNulls setByteBuffer(@NonNull String name, @Nullable ByteBuffer v) {
        if (v != null) {
            builder = builder.setByteBuffer(name, v);
        }
        return this;
    }

    public BoundStatementBuilderWrapperSkippingNulls setInstant(@NonNull String name, @Nullable Instant v) {
        if (v != null) {
            builder = builder.setInstant(name, v);
        }
        return this;
    }

    public <ElementT> BoundStatementBuilderWrapperSkippingNulls setSet(@NonNull String name, @Nullable Set<ElementT> v, @NonNull Class<ElementT> elementsClass) {
        if (v != null) {
            builder = builder.setSet(name, v, elementsClass);
        }
        return this;
    }

    public BoundStatement build() {
        return builder.build();
    }

    public BoundStatementBuilder getUnderlyingBuilder() {
        return builder;
    }
}
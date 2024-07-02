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

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Set;
import java.util.function.Function;

/**
* This wrapper skips null values to avoid tombstones creation in Cassandra tables
*/
public class BoundStatementBuilderWrapper {
    private BoundStatementBuilder builder;

    public BoundStatementBuilderWrapper setConsistencyLevel(@Nullable ConsistencyLevel consistencyLevel) {
        builder.setConsistencyLevel(consistencyLevel);
        return this;
    }

    public BoundStatementBuilderWrapper setTimeout(@Nullable Duration timeout) {
        builder.setTimeout(timeout);
        return this;
    }

    private BoundStatementBuilderWrapper(PreparedStatement statement) {
        builder = statement.boundStatementBuilder();
    }

    public BoundStatementBuilderWrapper setBoolean(@NonNull String name, boolean v) {
        builder = builder.setBoolean(name, v);
        return this;
    }

    public BoundStatementBuilderWrapper setByte(@NonNull String name, byte v) {
        builder = builder.setByte(name, v);
        return this;
    }

    public BoundStatementBuilderWrapper setInt(@NonNull String name, int v) {
        builder = builder.setInt(name, v);
        return this;
    }

    public BoundStatementBuilderWrapper setLong(@NonNull String name, long v) {
        builder = builder.setLong(name, v);
        return this;
    }

    public BoundStatementBuilderWrapper setInstant(@NonNull String name, @Nullable Instant v) {
        if (v != null) {
            builder = builder.setInstant(name, v);
        }
        return this;
    }

    public BoundStatementBuilderWrapper setLocalDate(@NonNull String name, @Nullable LocalDate v) {
        if (v != null) {
            builder = builder.setLocalDate(name, v);
        }
        return this;
    }

    public BoundStatementBuilderWrapper setLocalTime(@NonNull String name, @Nullable LocalTime v) {
        if (v != null) {
            builder = builder.setLocalTime(name, v);
        }
        return this;
    }

    public BoundStatementBuilderWrapper setByteBuffer(@NonNull String name, @Nullable ByteBuffer v) {
        if (v != null) {
            builder = builder.setByteBuffer(name, v);
        }
        return this;
    }

    public BoundStatementBuilderWrapper setString(@NonNull String name, @Nullable String v) {
        if (v != null) {
            builder = builder.setString(name, v);
        }
        return this;
    }

    public <ElementT> BoundStatementBuilderWrapper setSet(@NonNull String name, @Nullable Set<ElementT> v, @NonNull Class<ElementT> elementsClass) {
        if (v != null) {
            builder = builder.setSet(name, v, elementsClass);
        }
        return this;
    }

    public BoundStatementBuilderWrapper apply(Function<BoundStatementBuilder, BoundStatementBuilder> func) {
        builder = func.apply(builder);
        return this;
    }

    public BoundStatement build() {
        return builder.build();
    }

    public static BoundStatementBuilderWrapper builder(PreparedStatement statement) {
        return new BoundStatementBuilderWrapper(statement);
    }
}
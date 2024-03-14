/*
 * Copyright 2020-2024 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.cradle.testevents;

import com.exactpro.cradle.utils.CradleStorageException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.time.Instant;

import static com.exactpro.cradle.CoreStorageSettings.DEFAULT_BOOK_REFRESH_INTERVAL_MILLIS;

/**
 * Holds basic information about test event prepared to be stored in Cradle. Events extend this class with additional data
 */
public abstract class TestEventToStore implements TestEvent {
    protected @Nonnull final StoredTestEventId id;
    protected @Nonnull final String name;
    protected @Nullable final StoredTestEventId parentId;
    protected @Nonnull final String type;
    protected @Nullable final Instant endTimestamp;
    protected final boolean success;

    TestEventToStore(@Nonnull StoredTestEventId id,
                            @Nonnull String name,
                            @Nullable StoredTestEventId parentId,
                            @Nonnull String type,
                            @Nullable Instant endTimestamp,
                            boolean success) throws CradleStorageException {
        this.id = requireNonNull(id, "Id can't be null");
        this.name = requireNonNull(name, "Name can't be null");
        this.type = requireNonNull(type, "Type can't be null");
        this.endTimestamp = endTimestamp;
        this.parentId = parentId;
        this.success = success;
    }

    public static TestEventSingleToStoreBuilder singleBuilder(long storeActionRejectionThreshold) {
        return new TestEventSingleToStoreBuilder(storeActionRejectionThreshold);
    }

    public static TestEventSingleToStoreBuilder singleBuilder() {
        return singleBuilder(DEFAULT_BOOK_REFRESH_INTERVAL_MILLIS);
    }

    public static TestEventBatchToStoreBuilder batchBuilder(int maxBatchSize, long storeActionRejectionThreshold) {
        return new TestEventBatchToStoreBuilder(maxBatchSize, storeActionRejectionThreshold);
    }

    public static TestEventBatchToStoreBuilder batchBuilder(int maxBatchSize) {
        return batchBuilder(maxBatchSize, DEFAULT_BOOK_REFRESH_INTERVAL_MILLIS);
    }

    @Nonnull
    @Override
    public StoredTestEventId getId() {
        return id;
    }

    @Nonnull
    @Override
    public String getName() {
        return name;
    }

    @Nullable
    @Override
    public StoredTestEventId getParentId() {
        return parentId;
    }


    @Nonnull
    @Override
    public String getType() {
        return type;
    }

    @Nullable
    @Override
    public Instant getEndTimestamp() {
        return endTimestamp;
    }

    @Override
    public boolean isSuccess() {
        return success;
    }


    public final boolean isSingle() {
        return this instanceof TestEventSingleToStore;
    }

    public final boolean isBatch() {
        return this instanceof TestEventBatchToStore;
    }

    public final TestEventSingleToStore asSingle() {
        return (TestEventSingleToStore) this;
    }

    public final TestEventBatchToStore asBatch() {
        return (TestEventBatchToStore) this;
    }

    protected static <T> T requireNonNull(T obj, String message) throws CradleStorageException {
        if (obj == null) {
            throw new CradleStorageException(message);
        }
        return obj;
    }
}

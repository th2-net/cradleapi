/*
 *  Copyright 2023 Exactpro (Exactpro Systems Limited)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.exactpro.cradle.testevents;

import com.exactpro.cradle.utils.CradleStorageException;
import com.exactpro.cradle.utils.TestEventUtils;

import java.time.Instant;

import static com.exactpro.cradle.CoreStorageSettings.DEFAULT_BOOK_REFRESH_INTERVAL_MILLIS;

/**
 * Holds basic information about test event prepared to be stored in Cradle. Events extend this class with additional data
 */
public abstract class TestEventToStore implements TestEvent {
    protected final StoredTestEventId id;
    protected final String name;
    protected final StoredTestEventId parentId;
    protected String type;
    protected Instant endTimestamp;
    protected boolean success;

    public TestEventToStore(StoredTestEventId id, String name, StoredTestEventId parentId, long storeActionRejectionThreshold) throws CradleStorageException {
        this.id = id;
        this.name = name;
        this.parentId = parentId;
        TestEventUtils.validateTestEvent(this, storeActionRejectionThreshold);
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

    @Override
    public StoredTestEventId getId() {
        return id;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public StoredTestEventId getParentId() {
        return parentId;
    }


    @Override
    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }


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
}

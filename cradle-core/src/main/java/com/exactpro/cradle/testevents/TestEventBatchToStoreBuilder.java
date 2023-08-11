/*
 * Copyright 2020-2021 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.cradle.BookId;
import com.exactpro.cradle.utils.CradleStorageException;

import java.time.Instant;
import java.util.UUID;

/**
 * Builder for {@link TestEventBatchToStore} object. After calling {@link #build()} method, the builder can be reused to build new test event
 */
public class TestEventBatchToStoreBuilder {
    private final int maxBatchSize;
    private StoredTestEventId id;
    private String name;
    private StoredTestEventId parentId;
    private String type;

    private final long storeActionRejectionThreshold;

    public TestEventBatchToStoreBuilder(int maxBatchSize, long storeActionRejectionThreshold) {
        this.maxBatchSize = maxBatchSize;
        this.storeActionRejectionThreshold = storeActionRejectionThreshold;
    }


    public TestEventBatchToStoreBuilder id(StoredTestEventId id) {
        this.id = id;
        return this;
    }

    public TestEventBatchToStoreBuilder id(BookId book, String scope, Instant startTimestamp, String id) {
        this.id = new StoredTestEventId(book, scope, startTimestamp, id);
        return this;
    }

    public TestEventBatchToStoreBuilder idRandom(BookId book, String scope) {
        this.id = new StoredTestEventId(book, scope, Instant.now(), UUID.randomUUID().toString());
        return this;
    }

    public TestEventBatchToStoreBuilder name(String name) {
        this.name = name;
        return this;
    }

    public TestEventBatchToStoreBuilder parentId(StoredTestEventId parentId) {
        this.parentId = parentId;
        return this;
    }

    public TestEventBatchToStoreBuilder type(String type) {
        this.type = type;
        return this;
    }


    public TestEventBatchToStore build() throws CradleStorageException {
        try {
            TestEventBatchToStore result = createTestEventToStore(id, name, parentId);
            result.setType(type);
            return result;
        } finally {
            reset();
        }
    }


    protected TestEventBatchToStore createTestEventToStore(StoredTestEventId id, String name, StoredTestEventId parentId) throws CradleStorageException {
        return new TestEventBatchToStore(id, name, parentId, maxBatchSize, storeActionRejectionThreshold);
    }

    protected void reset() {
        id = null;
        name = null;
        parentId = null;
        type = null;
    }
}

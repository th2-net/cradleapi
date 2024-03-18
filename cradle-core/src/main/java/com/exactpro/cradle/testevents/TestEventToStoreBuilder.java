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

package com.exactpro.cradle.testevents;

import com.exactpro.cradle.BookId;
import com.exactpro.cradle.utils.CradleStorageException;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.Objects.requireNonNull;

public abstract class TestEventToStoreBuilder {
    private static final String BASE_UUID = UUID.randomUUID().toString();
    private static final AtomicLong ID_COUNTER = new AtomicLong();

    protected StoredTestEventId id;
    protected StoredTestEventId parentId;
    private final long storeActionRejectionThreshold;

    protected TestEventToStoreBuilder(long storeActionRejectionThreshold) {
        this.storeActionRejectionThreshold = storeActionRejectionThreshold;
    }

    public TestEventToStoreBuilder id(StoredTestEventId id) {
        checkParentEventId(id, this.parentId);
        Instant now = Instant.now();
        if (id.getStartTimestamp().isAfter(now.plusMillis(storeActionRejectionThreshold))) {
            throw new IllegalArgumentException(
                    "Event start timestamp (" + id.getStartTimestamp() +
                            ") is greater than current timestamp ( " + now + " ) plus storeActionRejectionThreshold interval (" + storeActionRejectionThreshold + ")ms");
        }
        this.id = requireNonNull(id, "Id can't be null");
        return this;
    }

    public TestEventToStoreBuilder id(BookId book, String scope, Instant startTimestamp, String id) {
        return id(new StoredTestEventId(book, scope, startTimestamp, id));
    }

    public TestEventToStoreBuilder idRandom(BookId book, String scope) {
        return id(new StoredTestEventId(book, scope, Instant.now(), BASE_UUID + ID_COUNTER.incrementAndGet()));
    }

    public TestEventToStoreBuilder parentId(StoredTestEventId parentId) {
        checkParentEventId(this.id, parentId);
        this.parentId = parentId;
        return this;
    }

    public StoredTestEventId getId() {
        return id;
    }

    public StoredTestEventId getParentId() {
        return parentId;
    }

    public abstract TestEventToStore build() throws CradleStorageException;

    protected void reset() {
        id = null;
        parentId = null;
    }

    private static void checkParentEventId(StoredTestEventId id, StoredTestEventId parentId) {
        if (id == null || parentId == null) {
            return;
        }
        if (id.equals(parentId)) throw new IllegalArgumentException("Test event cannot reference itself");
        if (!id.getBookId().equals(parentId.getBookId())) {
            throw new IllegalArgumentException("Test event and its parent must be from the same book");
        }
        if (!id.getScope().equals(parentId.getScope())) {
            throw new IllegalArgumentException("Test event and its parent must be from the same scope");
        }
    }
}

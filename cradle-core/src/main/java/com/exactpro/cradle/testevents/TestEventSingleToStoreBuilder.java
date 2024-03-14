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

import com.exactpro.cradle.BookId;
import com.exactpro.cradle.messages.StoredMessageId;
import com.exactpro.cradle.utils.CradleStorageException;

import java.time.Instant;
import java.util.HashSet;
import java.util.Set;

import static org.apache.commons.lang3.StringUtils.isEmpty;

/**
 * Builder for {@link TestEventSingleToStore} object. After calling {@link #build()} method, the builder can be reused to build new test event
 */
public class TestEventSingleToStoreBuilder extends TestEventToStoreBuilder {
    static final byte[] EMPTY_CONTENT = new byte[0];
    private String name;
    private String type = "";
    private Instant endTimestamp;
    private boolean success = true;
    private final Set<StoredMessageId> messages = new HashSet<>();
    private byte[] content = EMPTY_CONTENT;

    public TestEventSingleToStoreBuilder(long storeActionRejectionThreshold) {
        super(storeActionRejectionThreshold);
    }

    public TestEventSingleToStoreBuilder id(StoredTestEventId id) {
        checkMessageIds(id, this.messages);
        super.id(id);
        return this;
    }

    public TestEventSingleToStoreBuilder id(BookId book, String scope, Instant startTimestamp, String id) {
        super.id(book, scope, startTimestamp, id);
        return this;
    }

    @Override
    public TestEventSingleToStoreBuilder idRandom(BookId book, String scope) {
        super.idRandom(book, scope);
        return this;
    }

    public TestEventSingleToStoreBuilder name(String name) {
        if (isEmpty(name)) {
            throw new IllegalArgumentException("Name can't be null or empty");
        }
        this.name = name;
        return this;
    }

    public TestEventSingleToStoreBuilder parentId(StoredTestEventId parentId) {
        super.parentId(parentId);
        return this;
    }

    public TestEventSingleToStoreBuilder type(String type) {
        this.type = type == null ? "" : type;
        return this;
    }

    public TestEventSingleToStoreBuilder endTimestamp(Instant endTimestamp) {
        if (endTimestamp != null && endTimestamp.isBefore(id.getStartTimestamp())) {
            throw new IllegalArgumentException("Test event cannot end (" + endTimestamp +
                    ") sooner than it started (" + id.getStartTimestamp() + ')');
        }
        checkEndTimestamp(id, endTimestamp);
        this.endTimestamp = endTimestamp;
        return this;
    }

    public TestEventSingleToStoreBuilder success(boolean success) {
        this.success = success;
        return this;
    }

    public TestEventSingleToStoreBuilder messages(Set<StoredMessageId> ids) {
        checkMessageIds(this.id, ids);
        this.messages.addAll(ids);
        return this;
    }

    public TestEventSingleToStoreBuilder message(StoredMessageId id) {
        checkMessageId(this.id, id);
        this.messages.add(id);
        return this;
    }

    public TestEventSingleToStoreBuilder content(byte[] content) {
        this.content = content == null ? EMPTY_CONTENT : content;
        return this;
    }


    public TestEventSingleToStore build() throws CradleStorageException {
        try {
            return new TestEventSingleToStore(
                    id,
                    name,
                    parentId,
                    type,
                    endTimestamp,
                    success,
                    messages,
                    content
            );
        } finally {
            reset();
        }
    }

    protected void reset() {
        super.reset();
        id = null;
        name = null;
        parentId = null;
        type = "";
        endTimestamp = null;
        success = true;
        messages.clear();
        content = EMPTY_CONTENT;
    }

    private static void checkMessageIds(StoredTestEventId id, Set<StoredMessageId> msgIds) {
        if (id == null || msgIds == null || msgIds.isEmpty()) {
            return;
        }

        for (StoredMessageId msgId : msgIds) {
            if (!id.getBookId().equals(msgId.getBookId())) {
                throw new IllegalStateException("Book of message '" + id +
                        "' differs from test event book (" + id.getBookId() + ")");
            }
        }
    }

    private static void checkMessageId(StoredTestEventId id, StoredMessageId msgId) {
        if (id == null || msgId == null) {
            return;
        }

        if (!id.getBookId().equals(msgId.getBookId())) {
            throw new IllegalStateException("Book of message '" + id +
                    "' differs from test event book (" + id.getBookId() + ")");
        }
    }

    private static void checkEndTimestamp(StoredTestEventId id, Instant endTimestamp) {
        if (id == null || endTimestamp == null) {
            return;
        }

        if (endTimestamp.isBefore(id.getStartTimestamp())) {
            throw new IllegalStateException("Test event cannot end (" + endTimestamp +
                    ") sooner than it started (" + id.getStartTimestamp() + ')');
        }
    }
}

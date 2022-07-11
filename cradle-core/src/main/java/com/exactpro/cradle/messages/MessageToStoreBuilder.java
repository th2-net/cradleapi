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

package com.exactpro.cradle.messages;

import java.time.Instant;

import com.exactpro.cradle.BookId;
import com.exactpro.cradle.Direction;
import com.exactpro.cradle.utils.CradleStorageException;

/**
 * Builder for MessageToStore object. After calling {@link #build()} method, the builder can be reused to build new message
 */
public class MessageToStoreBuilder {
    private BookId bookId;
    private String sessionAlias;
    private Direction direction;
    private Instant timestamp;
    private long sequence;
    private byte[] content;
    private String protocol;
    private MessageMetadata metadata;


    public MessageToStoreBuilder bookId(BookId bookId) {
        this.bookId = bookId;
        return this;
    }

    public MessageToStoreBuilder sessionAlias(String sessionAlias) {
        this.sessionAlias = sessionAlias;
        return this;
    }

    public MessageToStoreBuilder direction(Direction direction) {
        this.direction = direction;
        return this;
    }

    public MessageToStoreBuilder timestamp(Instant timestamp) {
        this.timestamp = timestamp;
        return this;
    }

    public MessageToStoreBuilder sequence(long sequence) {
        this.sequence = sequence;
        return this;
    }

    public MessageToStoreBuilder id(StoredMessageId id) {
        this.bookId = id.getBookId();
        this.sessionAlias = id.getSessionAlias();
        this.direction = id.getDirection();
        this.timestamp = id.getTimestamp();
        this.sequence = id.getSequence();
        return this;
    }

    public MessageToStoreBuilder id(BookId bookId, String sessionAlias, Direction direction, Instant timestamp, long sequence) {
        this.bookId = bookId;
        this.sessionAlias = sessionAlias;
        this.direction = direction;
        this.timestamp = timestamp;
        this.sequence = sequence;
        return this;
    }

    public MessageToStoreBuilder content(byte[] content) {
        this.content = content;
        return this;
    }

    public MessageToStoreBuilder protocol(String protocol) {
        this.protocol = protocol;
        return this;
    }

    public MessageToStoreBuilder metadata(String key, String value) {
        if (metadata == null)
            metadata = new MessageMetadata();
        metadata.add(key, value);
        return this;
    }


    public MessageToStore build() throws CradleStorageException {
        try {
            MessageToStore result = createMessageToStore();
            result.setMetadata(metadata);
            return result;
        } finally {
            reset();
        }
    }


    protected MessageToStore createMessageToStore() throws CradleStorageException {
    	StoredMessageId id = new StoredMessageId(bookId, sessionAlias, direction, timestamp, sequence);
        return new MessageToStore(id, protocol, content);
    }

    protected void reset() {
        bookId = null;
        sessionAlias = null;
        direction = null;
        timestamp = null;
        sequence = 0;
        content = null;
        metadata = null;
    }
}

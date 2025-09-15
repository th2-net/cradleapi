/*
 * Copyright 2020-2025 Exactpro (Exactpro Systems Limited)
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
import com.exactpro.cradle.utils.TestEventUtils;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Set;

/**
 * Holds information about single (individual) test event prepared to be stored in Cradle
 */
public class TestEventSingleToStore extends TestEventToStore implements TestEventSingle {
    private Set<StoredMessageId> messages;
    private byte[] content;

    public TestEventSingleToStore(StoredTestEventId id, String name, StoredTestEventId parentId, long storeActionRejectionThreshold) throws CradleStorageException {
        super(id, name, parentId, storeActionRejectionThreshold);
    }


    public static TestEventSingleToStoreBuilder builder(long storeActionRejectionThreshold) {
        return new TestEventSingleToStoreBuilder(storeActionRejectionThreshold);
    }


    @Override
    public Set<StoredMessageId> getMessages() {
        return messages;
    }

    @Override
    @Deprecated
    public byte[] getContent() {
        return content;
    }

    public void setContent(byte[] content) {
        this.content = content;
    }

    @Override
    public ByteBuffer getContentBuffer() {
        if (content == null) { return null; }
        return ByteBuffer.wrap(content).asReadOnlyBuffer();
    }


    public void setEndTimestamp(Instant endTimestamp) throws CradleStorageException {
        this.endTimestamp = endTimestamp;
        TestEventUtils.validateTestEventEndDate(this);
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public void setMessages(Set<StoredMessageId> messages) throws CradleStorageException {
        validateAttachedMessageIds(messages);
        this.messages = messages;
    }

    private void validateAttachedMessageIds(Set<StoredMessageId> ids) throws CradleStorageException {
        if (ids == null)
            return;
        BookId eventBookId = getId().getBookId();
        for (StoredMessageId id : ids) {
            BookId messageBookId = id.getBookId();
            if (!eventBookId.equals(messageBookId))
                throw new CradleStorageException("Book of message (" +
                        messageBookId + ") differs from the event book (" + eventBookId + ")");
        }
    }

    @Override
    public String toString() {
        String contentAsText = content == null
                ? null :StandardCharsets.UTF_8.decode(ByteBuffer.wrap(content)).toString();
        return "TestEventSingleToStore{" +
                "endTimestamp=" + endTimestamp +
                ", success=" + success +
                ", messages=" + messages +
                ", content=" + contentAsText +
                '}';
    }
}

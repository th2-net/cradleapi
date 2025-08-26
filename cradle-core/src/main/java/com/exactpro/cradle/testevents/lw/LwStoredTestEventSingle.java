/*
 * Copyright 2025 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.cradle.testevents.lw;

import com.exactpro.cradle.PageId;
import com.exactpro.cradle.messages.StoredMessageId;
import com.exactpro.cradle.testevents.StoredTestEvent;
import com.exactpro.cradle.testevents.StoredTestEventId;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Objects;
import java.util.Set;

import static java.util.Collections.unmodifiableSet;

/**
 * Holds information about single (individual) test event stored in Cradle
 */
public class LwStoredTestEventSingle extends StoredTestEvent implements LwTestEventSingle {
    private final Instant endTimestamp;
    private final boolean success;
    private final Set<StoredMessageId> messages; // TODO: should be simplified
    private final ByteBuffer content;

    public LwStoredTestEventSingle(StoredTestEventId id, String name, String type, StoredTestEventId parentId,
                                   Instant endTimestamp, boolean success, ByteBuffer content, Set<StoredMessageId> eventMessages, PageId pageId, String error, Instant recDate) {
        super(id, name, type, parentId, pageId, error, recDate);

        this.endTimestamp = endTimestamp;
        this.success = success;

        if (content == null) {
            this.content = null;
        } else {
            if (!content.hasArray()) {
                throw new IllegalArgumentException(name + '.' + type + " event content hasn't got array");
            }
            this.content = ByteBuffer.wrap(content.array(), content.position(), content.remaining());
        }

        this.messages = eventMessages == null || eventMessages.isEmpty() ? null : unmodifiableSet(eventMessages);
    }

    public LwStoredTestEventSingle(LwTestEventSingle event, PageId pageId) {
        this(event.getId(), event.getName(), event.getType(), event.getParentId(),
                event.getEndTimestamp(), event.isSuccess(), event.getContent(), event.getMessages(), pageId, null, null);
    }


    @Override
    public Instant getEndTimestamp() {
        return endTimestamp;
    }

    @Override
    public boolean isSuccess() {
        return success;
    }

    @Override
    public Set<StoredMessageId> getMessages() {
        return messages;
    }

    @Override
    public ByteBuffer getContent() {
        return content;
    }

    @Override
    public Instant getLastStartTimestamp() {
        return getStartTimestamp();
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;

        LwStoredTestEventSingle that = (LwStoredTestEventSingle) o;
        return Objects.equals(endTimestamp, that.endTimestamp)
                && success == that.success
                && Objects.equals(messages, that.messages)
                && Objects.equals(content, that.content);
    }

    @Override
    public int hashCode() {
        int result = Objects.hashCode(endTimestamp);
        result = 31 * result + Boolean.hashCode(success);
        result = 31 * result + Objects.hashCode(messages);
        result = 31 * result + Objects.hashCode(content);
        return result;
    }

    @Override
    public String toString() {
        return "LwStoredTestEventSingle{" +
                "endTimestamp=" + endTimestamp +
                ", success=" + success +
                ", messages=" + messages +
                ", content=" + content +
                '}';
    }
}

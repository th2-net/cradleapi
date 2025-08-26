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
import com.exactpro.cradle.testevents.StoredTestEventId;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;

/**
 * Holds information about one lw test event stored in lw batch of events ({@link LwTestEventBatch})
 */
public class LwBatchedStoredTestEvent extends LwStoredTestEventSingle {
    private final transient LwTestEventBatch batch;

    public LwBatchedStoredTestEvent(LwTestEventSingle event, LwTestEventBatch batch, PageId pageId) {
        super(event, pageId);
        this.batch = batch;
    }

    // TODO: previous version were protected
    public LwBatchedStoredTestEvent(StoredTestEventId id, String name, String type, StoredTestEventId parentId,
                                    Instant endTimestamp, boolean success, ByteBuffer content,
                                    LwTestEventBatch batch, PageId pageId) {
        super(id, name, type, parentId, endTimestamp, success, content, Collections.emptySet(), pageId, "", Instant.now());
        this.batch = batch;
    }

    @Override
    public Set<StoredMessageId> getMessages() {
        if (batch == null) {
            return Collections.emptySet();
        }
        return batch.getMessages(this.getId());
    }

    public StoredTestEventId getBatchId() {
        return batch.getId();
    }

    public boolean hasChildren() {
        return batch.hasChildren(getId());
    }

    public Collection<LwBatchedStoredTestEvent> getChildren() {
        return batch.getChildren(getId());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof LwBatchedStoredTestEvent)) return false;
        LwBatchedStoredTestEvent that = (LwBatchedStoredTestEvent) o;
        return Objects.equals(getId(), that.getId())
                && Objects.equals(getParentId(), that.getParentId())
                && Objects.equals(getPageId(), that.getPageId())
                && Objects.equals(getName(), that.getName())
                && Objects.equals(getType(), that.getType())
                && Objects.equals(getEndTimestamp(), that.getEndTimestamp())
                && isSuccess() == that.isSuccess()
                && Objects.equals(getContent(), that.getContent());
    }

    @Override
    public int hashCode() {
        int result = Objects.hashCode(getId());
        result = 31 * result + Objects.hashCode(getParentId());
        result = 31 * result + Objects.hashCode(getPageId());
        result = 31 * result + Objects.hashCode(getName());
        result = 31 * result + Objects.hashCode(getType());
        result = 31 * result + Objects.hashCode(getEndTimestamp());
        result = 31 * result + Objects.hashCode(isSuccess());
        result = 31 * result + Objects.hashCode(getContent());
        return result;
    }

    @Override
    public String toString() {
        return "BatchedStoredTestEvent{" +
                "id=" + getId() +
                ", name='" + getName() + '\'' +
                ", type='" + getType() + '\'' +
                ", parentId=" + getParentId() +
                ", endTimestamp=" + getEndTimestamp() +
                ", success=" + isSuccess() +
                ", content=" + getContent() +
                ", pageId=" + getPageId() +
                '}';
    }
}

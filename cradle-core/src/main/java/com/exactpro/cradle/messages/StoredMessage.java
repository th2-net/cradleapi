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

import com.exactpro.cradle.BookId;
import com.exactpro.cradle.Direction;
import com.exactpro.cradle.PageId;
import com.exactpro.cradle.serialization.MessagesSizeCalculator;
import com.exactpro.cradle.utils.CompressionUtils;

import java.io.Serializable;
import java.time.Instant;
import java.util.Arrays;
import java.util.Objects;

/**
 * Holds information about one message stored in Cradle.
 */
public class StoredMessage implements Serializable, CradleMessage {
    private static final long serialVersionUID = 5602557739148866986L;

    private final StoredMessageId id;
    private final byte[] content;
    private final StoredMessageMetadata metadata;
    private final PageId pageId;
    private final String protocol;
    private final int serializedSize;

    public StoredMessage(CradleMessage message, StoredMessageId id, PageId pageId) {
        this(id, message.getProtocol(), message.getContent(), message.getMetadata() != null
                ? new StoredMessageMetadata(message.getMetadata()) : null, pageId);
    }

    public StoredMessage(StoredMessage copyFrom) {
        this(copyFrom, copyFrom.getId(), copyFrom.getPageId());
    }

    protected StoredMessage(StoredMessageId id, String protocol, byte[] content, StoredMessageMetadata metadata, PageId pageId) {
        this.id = id;
        this.protocol = protocol;
        this.content = content;
        this.metadata = metadata;
        this.pageId = pageId;
        this.serializedSize = MessagesSizeCalculator.calculateMessageSizeInBatch(this);
    }

    /**
     * @return unique message ID as stored in Cradle.
     * Result of this method should be used for referencing stored messages to obtain them from Cradle
     */
    public StoredMessageId getId() {
        return id;
    }

    @Override
    public BookId getBookId() {
        return id.getBookId();
    }

    @Override
    public String getSessionAlias() {
        return id.getSessionAlias();
    }

    @Override
    public Direction getDirection() {
        return id.getDirection();
    }

    @Override
    public Instant getTimestamp() {
        return id.getTimestamp();
    }

    @Override
    public long getSequence() {
        return id.getSequence();
    }

    @Override
    public byte[] getContent() {
        return content;
    }

    @Override
    public String getProtocol() {
        return protocol;
    }

    @Override
    public StoredMessageMetadata getMetadata() {
        return metadata;
    }

    public PageId getPageId() {
        return pageId;
    }

    @Override
    public int getSerializedSize() {
        return serializedSize;
    }

    @Override
    public int hashCode() {
		return Objects.hash(id, metadata, pageId, protocol, Arrays.hashCode(content));
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (! (obj instanceof StoredMessage))
        	return false;

		StoredMessage other = (StoredMessage) obj;

        return Objects.equals(id, other.id) &&
				Objects.equals(pageId, other.pageId) &&
				Objects.equals(protocol, other.protocol) &&
				Objects.equals(metadata, other.metadata) &&
				Arrays.equals(content, other.content);
    }

    @Override
    public String toString() {
        return new StringBuilder()
                .append("StoredMessage{").append(CompressionUtils.EOL)
                .append("id=").append(id).append(",").append(CompressionUtils.EOL)
                .append("content=").append(Arrays.toString(getContent())).append(CompressionUtils.EOL)
				.append("metadata=").append(getMetadata()).append(",").append(CompressionUtils.EOL)
				.append("protocol=").append(getProtocol()).append(",").append(CompressionUtils.EOL)
                .append("pageId=").append(getPageId()).append(CompressionUtils.EOL)
                .append("}").toString();
    }
}

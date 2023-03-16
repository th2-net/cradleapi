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

import com.exactpro.cradle.serialization.MessagesSizeCalculator;
import com.exactpro.cradle.utils.CompressionUtils;
import com.exactpro.cradle.utils.CradleStorageException;
import com.exactpro.cradle.utils.MessageUtils;

import java.util.Arrays;

/**
 * Object to hold information about one message prepared to be stored in Cradle
 */
public class MessageToStore implements CradleMessage {
    private final StoredMessageId id;
    private final String protocol;
    private final byte[] content;
    private MessageMetadata metadata;
    private int serializedSize;

    MessageToStore(StoredMessageId id, String protocol, byte[] content) throws CradleStorageException {
        this(id, protocol, content, null);
    }

    MessageToStore(StoredMessageId id, String protocol, byte[] content, MessageMetadata metadata) throws CradleStorageException {
        this.id = id;
        this.protocol = protocol;
        this.content = content;
        this.metadata = metadata;
        MessageUtils.validateMessage(this);
        this.serializedSize = MessagesSizeCalculator.calculateMessageSizeInBatch(this);
    }

    public MessageToStore(MessageToStore copyFrom) throws CradleStorageException {
        this(copyFrom.getId(), copyFrom.getProtocol(), copyFrom.getContent(), copyFrom.getMetadata() != null ? new MessageMetadata(copyFrom.getMetadata()) : null);
    }

    public static MessageToStoreBuilder builder() {
        return new MessageToStoreBuilder();
    }


    @Override
    public StoredMessageId getId() {
        return id;
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
    public MessageMetadata getMetadata() {
        return metadata;
    }

    @Override
    public int getSerializedSize() {
        return serializedSize;
    }

    public void setMetadata(MessageMetadata metadata) {
        this.metadata = metadata;
        this.serializedSize = MessagesSizeCalculator.calculateMessageSizeInBatch(this);
    }

    public void addMetadata(String key, String value) {
        if (metadata == null)
            metadata = new MessageMetadata();
        metadata.add(key, value);
        this.serializedSize = MessagesSizeCalculator.calculateMessageSizeInBatch(this);
    }


    @Override
    public String toString() {
        return new StringBuilder()
                .append("MessageToStore{").append(CompressionUtils.EOL)
                .append("id=").append(id).append(",").append(CompressionUtils.EOL)
                .append("content=").append(Arrays.toString(content)).append(CompressionUtils.EOL)
                .append("protocol=").append(protocol).append(",").append(CompressionUtils.EOL)
                .append("metadata=").append(metadata).append(",").append(CompressionUtils.EOL)
                .append("}").toString();
    }
}

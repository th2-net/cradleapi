/*
 * Copyright 2020-2023 Exactpro (Exactpro Systems Limited)
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
    private final MessageMetadata metadata;
    private final int serializedSize;

    private MessageToStore(StoredMessageId id, String protocol, byte[] content, MessageMetadata metadata, int serializedSize) throws CradleStorageException {
        this.id = id;
        this.protocol = protocol;
        this.content = content;
        this.metadata = metadata;
        if (serializedSize > 0) {
            this.serializedSize = serializedSize;
        } else {
            MessageUtils.validateMessage(this);
            this.serializedSize = MessagesSizeCalculator.calculateMessageSizeInBatch(this);
        }
    }

    MessageToStore(StoredMessageId id, String protocol, byte[] content, MessageMetadata metadata) throws CradleStorageException {
        this(id, protocol, content, metadata, -1);
    }

    public MessageToStore(MessageToStore copyFrom) throws CradleStorageException {
        this(
                copyFrom.getId(),
                copyFrom.getProtocol(),
                copyFrom.getContent(),
                copyFrom.getMetadata() != null ? new MessageMetadata(copyFrom.getMetadata()) : null,
                copyFrom.serializedSize
        );
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

    @Override
    public String toString() {
        return "MessageToStore{" + System.lineSeparator() +
                "id=" + id + "," + System.lineSeparator() +
                "content=" + Arrays.toString(content) + System.lineSeparator() +
                "protocol=" + protocol + "," + System.lineSeparator() +
                "metadata=" + metadata + "," + System.lineSeparator() +
                "serializedSize=" + serializedSize + "," + System.lineSeparator() +
                "}";
    }
}

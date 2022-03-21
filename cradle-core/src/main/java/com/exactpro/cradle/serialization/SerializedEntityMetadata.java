package com.exactpro.cradle.serialization;

import com.exactpro.cradle.messages.StoredMessage;

import java.time.Instant;

public class SerializedEntityMetadata {
    private final Instant timestamp;
    private final int serializedEntitySize;

    public SerializedEntityMetadata(Instant start, int serializedEntitySize) {
        this.timestamp = start;
        this.serializedEntitySize = serializedEntitySize;
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    public int getSerializedEntitySize() {
        return serializedEntitySize;
    }

    /**
     * Creates metadata for the serialized content of the specified {@link StoredMessage}.
     */
    public static SerializedEntityMetadata forMessage(StoredMessage message) {
        Instant timestamp = message.getTimestamp();
        int serializedMessageSize = MessagesSizeCalculator.calculateMessageSize(message);

        return new SerializedEntityMetadata(timestamp, serializedMessageSize);
    }
}

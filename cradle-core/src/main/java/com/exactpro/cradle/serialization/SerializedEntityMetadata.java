package com.exactpro.cradle.serialization;

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
}

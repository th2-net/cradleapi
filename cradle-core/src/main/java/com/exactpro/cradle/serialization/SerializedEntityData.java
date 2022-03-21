package com.exactpro.cradle.serialization;

import java.util.List;

public class SerializedEntityData {
    private final List<SerializedEntityMetadata> serializedEntityMetadata;
    private final byte[] serializedData;

    public SerializedEntityData(List<SerializedEntityMetadata> serializedEntityMetadata, byte[] serializedData) {
        this.serializedEntityMetadata = serializedEntityMetadata;
        this.serializedData = serializedData;
    }

    public List<SerializedEntityMetadata> getSerializedEntityMetadata() {
        return serializedEntityMetadata;
    }

    public byte[] getSerializedData() {
        return serializedData;
    }
}

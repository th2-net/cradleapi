package com.exactpro.cradle;

public enum SessionRecordType {
    SESSION(1),
    SESSION_GROUP(2);

    private final byte value;
    SessionRecordType(int value) {
        this.value = (byte) value;
    }

    public byte getValue() {
        return value;
    }

    /**
     * Returns SessionRecordType form value
     * @param value - value for which SessionRecordType should be resolved
     * @return SessionRecordType that corresponds to given value
     * @throws IllegalArgumentException if value does not match any SessionRecordType
     */
    public static SessionRecordType from(byte value) {
        for (SessionRecordType e: values())
            if (e.getValue() == value)
                return e;
        throw new IllegalArgumentException(String.format("No SessionRecordType is associated with value (%d)", value));
    }
}
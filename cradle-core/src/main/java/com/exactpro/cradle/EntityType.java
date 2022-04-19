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
package com.exactpro.cradle;

public enum EntityType {
    MESSAGE(1),
    EVENT(2);

    private final byte value;
    EntityType(int value) {
        this.value = (byte) value;
    }

    public byte getValue() {
        return value;
    }

    /**
     * Returns EntityType form value
     * @param value some Entity written in byte
     * @return EntityType that corresponds to given value
     * @throws IllegalArgumentException if value does not match any entity type
     */
    public static EntityType from(byte value) {
        for (EntityType e: values())
            if (e.getValue() == value)
                return e;
        throw new IllegalArgumentException(String.format("No entity associated with value (%d)", value));
    }
}
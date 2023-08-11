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
package com.exactpro.cradle.serialization;

import java.util.List;

public class SerializedEntityData<M extends SerializedEntityMetadata> {
    private final List<M> serializedEntityMetadata;
    private final byte[] serializedData;

    public SerializedEntityData(List<M> serializedEntityMetadata, byte[] serializedData) {
        this.serializedEntityMetadata = serializedEntityMetadata;
        this.serializedData = serializedData;
    }

    public List<M> getSerializedEntityMetadata() {
        return serializedEntityMetadata;
    }

    public byte[] getSerializedData() {
        return serializedData;
    }
}

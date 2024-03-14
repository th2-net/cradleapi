/*
 * Copyright 2021-2024 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.cradle.messages.GroupedMessageBatchToStore;
import com.exactpro.cradle.messages.MessageBatchToStore;
import com.exactpro.cradle.testevents.TestEventBatchToStore;
import com.exactpro.cradle.testevents.TestEventBatchToStoreBuilder;
import com.exactpro.cradle.testevents.TestEventSingleToStoreBuilder;

/**
 * Factory to create entities to be used with {@link CradleStorage}. Created objects will conform with particular CradleStorage settings.
 */
public class CradleEntitiesFactory {
    private final int maxMessageBatchSize,
            maxTestEventBatchSize;

    private final long storeActionRejectionThreshold;

    /**
     * Creates new factory for entities to be used with {@link CradleStorage}
     *
     * @param maxMessageBatchSize   maximum size of messages (in bytes) that {@link MessageBatchToStore} can hold
     * @param maxTestEventBatchSize maximum size of test events (in bytes) that {@link TestEventBatchToStore} can hold
     */
    public CradleEntitiesFactory(int maxMessageBatchSize, int maxTestEventBatchSize, long storeActionRejectionThreshold) {
        this.maxMessageBatchSize = maxMessageBatchSize;
        this.maxTestEventBatchSize = maxTestEventBatchSize;
        this.storeActionRejectionThreshold = storeActionRejectionThreshold;
    }


    @Deprecated
    public MessageBatchToStore messageBatch() {
        return new MessageBatchToStore(maxMessageBatchSize, storeActionRejectionThreshold);
    }

    public GroupedMessageBatchToStore groupedMessageBatch(String group) {
        return new GroupedMessageBatchToStore(group, maxMessageBatchSize, storeActionRejectionThreshold);
    }

    public TestEventBatchToStoreBuilder testEventBatchBuilder() {
        return new TestEventBatchToStoreBuilder(maxTestEventBatchSize, storeActionRejectionThreshold);
    }

    public TestEventSingleToStoreBuilder testEventBuilder() {
        return new TestEventSingleToStoreBuilder(storeActionRejectionThreshold);
    }

    public int getMaxMessageBatchSize() {
        return maxMessageBatchSize;
    }

    public int getMaxTestEventBatchSize() {
        return maxTestEventBatchSize;
    }
}

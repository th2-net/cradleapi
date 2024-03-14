/*
 * Copyright 2020-2024 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.cradle.testevents;

import com.exactpro.cradle.messages.StoredMessageId;
import com.exactpro.cradle.serialization.EventsSizeCalculator;
import com.exactpro.cradle.utils.CradleStorageException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.time.Instant;
import java.util.Set;


/**
 * Holds information about single (individual) test event prepared to be stored in Cradle
 */
public class TestEventSingleToStore extends TestEventToStore implements TestEventSingle {
    private @Nonnull final Set<StoredMessageId> messages;
    private @Nonnull final byte[] content;
    private final int size;

    TestEventSingleToStore(@Nonnull StoredTestEventId id,
                                  @Nonnull String name,
                                  @Nullable StoredTestEventId parentId,
                                  @Nonnull String type,
                                  @Nullable Instant endTimestamp,
                                  boolean success,
                                  @Nonnull Set<StoredMessageId> messages,
                                  @Nonnull byte[] content) throws CradleStorageException {
        super(id, name, parentId, type, endTimestamp, success);
        this.messages = Set.copyOf(requireNonNull(messages, "Messages can't be null"));
        this.content = requireNonNull(content, "Content can't be null");
        // Size calculation must be last instruction because function uses other class fields
        this.size = EventsSizeCalculator.calculateEventRecordSize(this);
    }

    public static TestEventSingleToStoreBuilder builder(long storeActionRejectionThreshold) {
        return new TestEventSingleToStoreBuilder(storeActionRejectionThreshold);
    }


    @Nonnull
    @Override
    public Set<StoredMessageId> getMessages() {
        return messages;
    }

    @Override
    public byte[] getContent() {
        return content;
    }

    public int getSize() {
        return size;
    }
}

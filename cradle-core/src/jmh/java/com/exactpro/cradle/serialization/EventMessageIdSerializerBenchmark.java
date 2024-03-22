/*
 * Copyright 2024 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.cradle.BookId;
import com.exactpro.cradle.Direction;
import com.exactpro.cradle.messages.StoredMessageId;
import com.exactpro.cradle.serialization.version2.EventMessageIdSerializer;
import com.exactpro.cradle.testevents.TestEventSingleToStore;
import com.exactpro.cradle.utils.CradleStorageException;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import static com.exactpro.cradle.CoreStorageSettings.DEFAULT_BOOK_REFRESH_INTERVAL_MILLIS;
import static org.openjdk.jmh.annotations.Mode.Throughput;

@State(Scope.Benchmark)
public class EventMessageIdSerializerBenchmark {
    private static final BookId BOOK_ID = new BookId("benchmark-book");
    private static final String SCOPE = "benchmark-scope";
    private static final String SESSION_ALIAS_PREFIX = "benchmark-alias-";
    private static final String EVENT_ID_PREFIX = "benchmark-event-";

    @State(Scope.Thread)
    public static class EventBatchState {
        @Param({"10", "100", "1000"})
        public int size;
        @Param({"1", "10"})
        public int aliases;
        @Param({"1", "10", "100"})
        public int idsPerDirection;

        private final Collection<TestEventSingleToStore> events = new ArrayList<>();
        @Setup
        public void init() throws CradleStorageException {
            int seqCounter = 0;
            for (int eventIndex = 0; eventIndex < size; eventIndex++) {
                Set<StoredMessageId> msgIds = new HashSet<>();
                for (int aliasIndex = 0; aliasIndex < aliases; aliasIndex++) {
                    for (Direction direction : Direction.values()) {
                        for (int msgIndex = 0; msgIndex < idsPerDirection; msgIndex++) {
                            msgIds.add(new StoredMessageId(BOOK_ID, SESSION_ALIAS_PREFIX + aliasIndex, direction, Instant.now(), ++seqCounter));
                        }
                    }
                }
                events.add(
                        TestEventSingleToStore.builder(DEFAULT_BOOK_REFRESH_INTERVAL_MILLIS)
                                .id(BOOK_ID, SCOPE, Instant.now(), EVENT_ID_PREFIX + eventIndex)
                                .name("benchmark-event")
                                .messages(msgIds)
                                .build()
                );
            }
        }
    }

    @State(Scope.Thread)
    public static class MessageIdsState {
        @Param({"1", "10"})
        public int aliases;
        @Param({"1", "10", "100"})
        public int idsPerDirection;
        private final Set<StoredMessageId> messageIds = new HashSet<>();
        @Setup
        public void init() {
            int seqCounter = 0;
            for (int aliasIndex = 0; aliasIndex < aliases; aliasIndex++) {
                for (Direction direction : Direction.values()) {
                    for (int msgIndex = 0; msgIndex < idsPerDirection; msgIndex++) {
                        messageIds.add(new StoredMessageId(BOOK_ID, SESSION_ALIAS_PREFIX + aliasIndex, direction, Instant.now(), ++seqCounter));
                    }
                }
            }
        }
    }

    @Benchmark
    @BenchmarkMode({Throughput})
    public void benchmarkSerializeBatchLinkedMessageIds(EventBatchState state) throws IOException {
        com.exactpro.cradle.serialization.version1.EventMessageIdSerializer.serializeBatchLinkedMessageIds(state.events);
    }

    @Benchmark
    @BenchmarkMode({Throughput})
    public void benchmarkSerializeLinkedMessageIds(MessageIdsState state) throws IOException {
        com.exactpro.cradle.serialization.version1.EventMessageIdSerializer.serializeLinkedMessageIds(state.messageIds);
    }

    @Benchmark
    @BenchmarkMode({Throughput})
    public void benchmarkSerializeBatchLinkedMessageIds2(EventBatchState state) {
        EventMessageIdSerializer.serializeBatchLinkedMessageIds(state.events);
    }

    @Benchmark
    @BenchmarkMode({Throughput})
    public void benchmarkSerializeLinkedMessageIds2(MessageIdsState state) throws IOException {
        EventMessageIdSerializer2.serializeLinkedMessageIds(state.messageIds);
    }
}

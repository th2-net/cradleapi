/*
 * Copyright 2025 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.cradle.utils;

import com.exactpro.cradle.BookId;
import com.exactpro.cradle.CoreStorageSettings;
import com.exactpro.cradle.testevents.StoredTestEventId;
import com.exactpro.cradle.testevents.TestEventBatchToStore;
import com.exactpro.cradle.testevents.TestEventSingleToStore;
import com.exactpro.cradle.testevents.TestEventSingleToStoreBuilder;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;
import org.testcontainers.shaded.org.apache.commons.lang3.RandomStringUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.UUID;

import static org.openjdk.jmh.annotations.Mode.Throughput;
import static org.openjdk.jmh.annotations.Scope.Thread;

@State(Scope.Benchmark)
public class TestEventUtilsBenchmark {

    @State(Thread)
    public static class TestEventState {
        private static final BookId BOOK_ID = new BookId("benchmark-book");
        private static final String SCOPE = "benchmark-scope";
        private static final int MAX_SIZE = 1_024 * 1_024;
        private static final long THRESHOLD = new CoreStorageSettings().calculateStoreActionRejectionThreshold();

        private StoredTestEventId batchId;
        private TestEventBatchToStore batch;
        private TestEventSingleToStore event;
        private ByteBuffer serializedEventBatch;

        @Param({"1024", "256000", "512000"})
        public int bodySize = 0;

        @Setup
        public void init() throws CradleStorageException {
            StoredTestEventId parentId = new StoredTestEventId(BOOK_ID, SCOPE, Instant.now(), UUID.randomUUID().toString());
            batchId = new StoredTestEventId(BOOK_ID, SCOPE, Instant.now(), UUID.randomUUID().toString());
            StoredTestEventId eventId = new StoredTestEventId(BOOK_ID, SCOPE, Instant.now(), UUID.randomUUID().toString());

            batch = new TestEventBatchToStore(batchId,
                    "Batch",
                    parentId,
                    MAX_SIZE,
                    THRESHOLD);

            event = new TestEventSingleToStoreBuilder(THRESHOLD)
                    .id(eventId)
                    .parentId(parentId)
                    .name("benchmark-event-name")
                    .type("benchmark-event-type")
                    .success(true)
                    .endTimestamp(Instant.now())
                    .content(RandomStringUtils.randomAlphabetic(bodySize).getBytes())
                    .build();

            batch.addTestEvent(event);

            serializedEventBatch = ByteBuffer.wrap(
                    TestEventUtils.serializeTestEvents(batch.getTestEvents()).getSerializedData());
        }
    }

    @Benchmark
    @BenchmarkMode(Throughput)
    public void benchmarkSerializeEvent(Blackhole blackhole, TestEventState state) {
        blackhole.consume(TestEventUtils.serializeTestEvent(state.event));
    }

    @Benchmark
    @BenchmarkMode(Throughput)
    public void benchmarkSerializeEventBatch(Blackhole blackhole, TestEventState state) {
        blackhole.consume(TestEventUtils.serializeTestEvents(state.batch.getTestEvents()));
    }

    @Benchmark
    @BenchmarkMode(Throughput)
    public void benchmarkDeserializeEventBatch(Blackhole blackhole, TestEventState state) throws IOException {
        blackhole.consume(TestEventUtils.deserializeTestEvents(state.serializedEventBatch, state.batchId));
    }
}

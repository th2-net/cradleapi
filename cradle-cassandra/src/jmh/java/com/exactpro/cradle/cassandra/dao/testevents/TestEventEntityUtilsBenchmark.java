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

package com.exactpro.cradle.cassandra.dao.testevents;

import com.exactpro.cradle.BookId;
import com.exactpro.cradle.CoreStorageSettings;
import com.exactpro.cradle.Direction;
import com.exactpro.cradle.PageId;
import com.exactpro.cradle.cassandra.dao.SerializedEntity;
import com.exactpro.cradle.messages.StoredMessageId;
import com.exactpro.cradle.serialization.SerializedEntityMetadata;
import com.exactpro.cradle.testevents.StoredTestEventId;
import com.exactpro.cradle.testevents.TestEventBatchToStore;
import com.exactpro.cradle.testevents.TestEventSingleToStore;
import com.exactpro.cradle.testevents.TestEventSingleToStoreBuilder;
import com.exactpro.cradle.utils.CompressException;
import com.exactpro.cradle.utils.CompressionType;
import com.exactpro.cradle.utils.CradleStorageException;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;
import org.testcontainers.shaded.org.apache.commons.lang3.RandomStringUtils;

import java.io.IOException;
import java.time.Instant;
import java.util.UUID;

import static org.openjdk.jmh.annotations.Mode.Throughput;
import static org.openjdk.jmh.annotations.Scope.Thread;

@State(Scope.Benchmark)
public class TestEventEntityUtilsBenchmark {
    private static final BookId BOOK_ID = new BookId("benchmark-book");
    private static final PageId PAGE_ID = new PageId(BOOK_ID, Instant.now(), "benchmark-page");
    private static final String SCOPE = "benchmark-scope";


    @State(Thread)
    public static class TestEventEntityState {
        private static final long THRESHOLD = new CoreStorageSettings().calculateStoreActionRejectionThreshold();
        private static final int MAX_SIZE = 1_024 * 1_024;
        private static final String SESSION_ALIAS = "benchmark-session-alias";

        private TestEventEntity eventEntity;
        private TestEventEntity batchEntity;

        @Param({"1024", "256000", "512000"})
        public int bodySize = 0;

        @Param({"true", "false"})
        public boolean compressed = true;

        @Param({"0", "1", "5"})
        public int messages = 0;

        @Setup
        public void init() throws CradleStorageException, IOException, CompressException {
            StoredTestEventId parentId = new StoredTestEventId(BOOK_ID, SCOPE, Instant.now(), UUID.randomUUID().toString());
            StoredTestEventId batchId = new StoredTestEventId(BOOK_ID, SCOPE, Instant.now(), UUID.randomUUID().toString());
            StoredTestEventId eventId = new StoredTestEventId(BOOK_ID, SCOPE, Instant.now(), UUID.randomUUID().toString());

            TestEventSingleToStoreBuilder eventBuilder = new TestEventSingleToStoreBuilder(THRESHOLD)
                    .id(eventId)
                    .parentId(parentId)
                    .name("benchmark-event-name")
                    .type("benchmark-event-type")
                    .success(true)
                    .endTimestamp(Instant.now())
                    .content(RandomStringUtils.randomAlphabetic(bodySize).getBytes());
            for (int i = 0; i < messages; i++) {
                eventBuilder.message(
                        new StoredMessageId(BOOK_ID, SESSION_ALIAS, Direction.FIRST, Instant.now(), System.nanoTime())
                );
            }
            TestEventSingleToStore event = eventBuilder.build();

            SerializedEntity<SerializedEntityMetadata, TestEventEntity> serializedEntity = TestEventEntityUtils
                    .toSerializedEntity(event, PAGE_ID, CompressionType.LZ4, compressed ? 0 : Integer.MAX_VALUE);
            eventEntity = serializedEntity.getEntity();

            TestEventBatchToStore batch = new TestEventBatchToStore(batchId,
                    "benchmark-event-batch-name",
                    parentId,
                    MAX_SIZE,
                    THRESHOLD);
            batch.addTestEvent(event);

            serializedEntity = TestEventEntityUtils
                    .toSerializedEntity(batch, PAGE_ID, CompressionType.LZ4, compressed ? 0 : Integer.MAX_VALUE);
            batchEntity = serializedEntity.getEntity();
        }
    }

    @Benchmark
    @BenchmarkMode(Throughput)
    public void benchmarkDeserializeEvent(Blackhole blackhole, TestEventEntityState state) {
        blackhole.consume(TestEventEntityUtils.toStoredTestEvent(state.eventEntity, PAGE_ID));
    }

    @Benchmark
    @BenchmarkMode(Throughput)
    public void benchmarkDeserializeLwEvent(Blackhole blackhole, TestEventEntityState state) {
        blackhole.consume(TestEventEntityUtils.toLwStoredTestEvent(state.eventEntity, PAGE_ID));
    }

    @Benchmark
    @BenchmarkMode(Throughput)
    public void benchmarkDeserializeBatch(Blackhole blackhole, TestEventEntityState state) {
        blackhole.consume(TestEventEntityUtils.toStoredTestEvent(state.batchEntity, PAGE_ID));
    }

    @Benchmark
    @BenchmarkMode(Throughput)
    public void benchmarkDeserializeLwBatch(Blackhole blackhole, TestEventEntityState state) {
        blackhole.consume(TestEventEntityUtils.toLwStoredTestEvent(state.batchEntity, PAGE_ID));
    }
}
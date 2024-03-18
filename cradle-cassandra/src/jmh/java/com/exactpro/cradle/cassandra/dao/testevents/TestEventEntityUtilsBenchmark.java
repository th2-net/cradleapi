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

package com.exactpro.cradle.cassandra.dao.testevents;

import com.exactpro.cradle.BookId;
import com.exactpro.cradle.Direction;
import com.exactpro.cradle.PageId;
import com.exactpro.cradle.messages.StoredMessageId;
import com.exactpro.cradle.testevents.StoredTestEventId;
import com.exactpro.cradle.testevents.TestEventBatchToStore;
import com.exactpro.cradle.testevents.TestEventBatchToStoreBuilder;
import com.exactpro.cradle.testevents.TestEventSingleToStore;
import com.exactpro.cradle.testevents.TestEventSingleToStoreBuilder;
import com.exactpro.cradle.utils.CompressException;
import com.exactpro.cradle.utils.CompressionType;
import com.exactpro.cradle.utils.CradleStorageException;
import org.apache.commons.lang3.RandomStringUtils;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.io.IOException;
import java.time.Instant;
import java.util.UUID;

import static com.exactpro.cradle.CoreStorageSettings.DEFAULT_BOOK_REFRESH_INTERVAL_MILLIS;
import static com.exactpro.cradle.CradleStorage.DEFAULT_MAX_TEST_EVENT_BATCH_SIZE;
import static com.exactpro.cradle.cassandra.CassandraStorageSettings.DEFAULT_MAX_UNCOMPRESSED_MESSAGE_BATCH_SIZE;
import static org.openjdk.jmh.annotations.Mode.Throughput;

@State(Scope.Benchmark)
public class TestEventEntityUtilsBenchmark {
    private static final BookId BOOK_ID = new BookId("benchmark-book");
    private static final PageId PAGE_ID = new PageId(BOOK_ID, Instant.now(), "benchmark-page");
    private static final String SCOPE = "benchmark-scope";
    private static final String SESSION_ALIAS_PREFIX = "benchmark-alias-";
    private static final String EVENT_NAME_PREFIX = "benchmark-event-";
    private static final int CONTENT_SIZE = 500;
    private static final int EVENT_NUMBER = 100;
    private static final int SESSION_ALIAS_NUMBER = 5;
    private static final int MESSAGES_PER_DIRECTION = 2;
    @State(Scope.Thread)
    public static class EventBatchState {
        private TestEventBatchToStore batch;
        @Setup
        public void init() throws CradleStorageException {
            StoredTestEventId parentId = new StoredTestEventId(BOOK_ID, SCOPE, Instant.now(), UUID.randomUUID().toString());
            TestEventBatchToStoreBuilder batchBuilder = TestEventBatchToStore.builder(DEFAULT_MAX_TEST_EVENT_BATCH_SIZE, DEFAULT_BOOK_REFRESH_INTERVAL_MILLIS)
                    .id(BOOK_ID, SCOPE, Instant.now(), UUID.randomUUID().toString())
                    .parentId(parentId);

            int seqCounter = 0;
            for (int eventIndex = 0; eventIndex < EVENT_NUMBER; eventIndex++) {
                TestEventSingleToStoreBuilder eventBuilder = TestEventSingleToStore.builder(DEFAULT_BOOK_REFRESH_INTERVAL_MILLIS)
                        .id(BOOK_ID, SCOPE, Instant.now(), UUID.randomUUID().toString())
                        .parentId(parentId)
                        .name(EVENT_NAME_PREFIX + eventIndex)
                        .content(RandomStringUtils.random(CONTENT_SIZE, true, true).getBytes());

                for (int aliasIndex = 0; aliasIndex < SESSION_ALIAS_NUMBER; aliasIndex++) {
                    for (Direction direction : Direction.values()) {
                        for (int msgIndex = 0; msgIndex < MESSAGES_PER_DIRECTION; msgIndex++) {
                            eventBuilder.message(new StoredMessageId(BOOK_ID, SESSION_ALIAS_PREFIX + aliasIndex, direction, Instant.now(), ++seqCounter));
                        }
                    }
                }
                batchBuilder.addTestEvent(eventBuilder.build());
            }
            batch = batchBuilder.build();
        }
    }

    @Benchmark
    @BenchmarkMode({Throughput})
    public void benchmarkSerializeBatchLinkedMessageIds(EventBatchState state) throws IOException, CompressException {
        TestEventEntityUtils.toSerializedEntity(state.batch, PAGE_ID, CompressionType.LZ4, DEFAULT_MAX_UNCOMPRESSED_MESSAGE_BATCH_SIZE);
    }
}

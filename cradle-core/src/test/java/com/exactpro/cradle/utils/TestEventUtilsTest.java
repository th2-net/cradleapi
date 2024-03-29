/*
 * Copyright 2021-2023 Exactpro (Exactpro Systems Limited)
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
import com.exactpro.cradle.testevents.TestEventSingleToStoreBuilder;
import org.testng.annotations.Test;

import java.io.IOException;
import java.time.Instant;

public class TestEventUtilsTest {
    @Test
    public void toFromBytes() throws CradleStorageException, IOException {
        BookId book = new BookId("Book1");
        String scope = "Scope1";
        StoredTestEventId parentId = new StoredTestEventId(book, scope, Instant.now(), scope);
        StoredTestEventId batchId = new StoredTestEventId(book, scope, Instant.now(), "BatchID");
        long storeActionRejectionThreshold = new CoreStorageSettings().calculateStoreActionRejectionThreshold();

        TestEventBatchToStore batch = new TestEventBatchToStore(batchId,
                "Batch",
                parentId,
                1024,
                storeActionRejectionThreshold);

        batch.addTestEvent(new TestEventSingleToStoreBuilder(storeActionRejectionThreshold)
                .id(book, scope, Instant.now(), "EventID")
                .name("Event1")
                .parentId(parentId)
                .build());

        byte[] bytes = TestEventUtils.serializeTestEvents(batch.getTestEvents()).getSerializedData();
        TestEventUtils.deserializeTestEvents(bytes, batchId);
    }
}

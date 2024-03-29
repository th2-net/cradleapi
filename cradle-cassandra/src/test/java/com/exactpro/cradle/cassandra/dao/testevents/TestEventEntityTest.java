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

package com.exactpro.cradle.cassandra.dao.testevents;

import com.exactpro.cradle.BookId;
import com.exactpro.cradle.CoreStorageSettings;
import com.exactpro.cradle.Direction;
import com.exactpro.cradle.PageId;
import com.exactpro.cradle.cassandra.dao.SerializedEntity;
import com.exactpro.cradle.messages.StoredMessageId;
import com.exactpro.cradle.serialization.SerializedEntityMetadata;
import com.exactpro.cradle.testevents.StoredTestEvent;
import com.exactpro.cradle.testevents.StoredTestEventId;
import com.exactpro.cradle.testevents.TestEventBatchToStore;
import com.exactpro.cradle.testevents.TestEventSingleToStore;
import com.exactpro.cradle.testevents.TestEventSingleToStoreBuilder;
import com.exactpro.cradle.testevents.TestEventToStore;
import com.exactpro.cradle.utils.CompressException;
import com.exactpro.cradle.utils.CompressionType;
import com.exactpro.cradle.utils.CradleIdException;
import com.exactpro.cradle.utils.CradleStorageException;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.recursive.comparison.RecursiveComparisonConfiguration;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.time.Instant;
import java.util.HashSet;
import java.util.Set;
import java.util.zip.DataFormatException;

import static com.exactpro.cradle.cassandra.TestUtils.createContent;

public class TestEventEntityTest {
    private final BookId book = new BookId("Book1");
    private final PageId page = new PageId(book, Instant.now(), "Page1");
    private final String scope = "Scope1";
    private final Instant startTimestamp = Instant.now();

    private final long storeActionRejectionThreshold = new CoreStorageSettings().calculateStoreActionRejectionThreshold();
    private final StoredTestEventId eventId = new StoredTestEventId(book, scope, startTimestamp, "EventId"),
            parentId = new StoredTestEventId(book, scope, startTimestamp, "ParentEventId");

    private final TestEventSingleToStoreBuilder singleBuilder = TestEventSingleToStore.builder(storeActionRejectionThreshold);

    @DataProvider(name = "events")
    public Object[][] events() throws CradleStorageException {
        int contentLength = 20;
        int messagesLength = 10;
        TestEventBatchToStore batch = TestEventBatchToStore.builder(1024, storeActionRejectionThreshold)
                .id(new StoredTestEventId(book, scope, startTimestamp, "BatchId"))
                .parentId(parentId)
                .build();
        batch.addTestEvent(prepareSingle().content(createContent(contentLength)).build());
        return new Object[][]
                {
                        {prepareSingle().content(createContent(contentLength)).build()},
                        {prepareSingle().messages(createMessageIds(messagesLength)).build()},
                        {prepareSingle().content(createContent(contentLength)).messages(createMessageIds(messagesLength)).build()},
                        {batch}
                };
    }

    private TestEventSingleToStoreBuilder prepareSingle() {
        return singleBuilder
                .id(eventId)
                .parentId(parentId)
                .name("TestEvent1")
                .type("Type1");
    }

    private Set<StoredMessageId> createMessageIds(int size) {
        Set<StoredMessageId> result = new HashSet<>();
        for (int i = 0; i < size; i++)
            result.add(new StoredMessageId(book, "Session1", Direction.FIRST, startTimestamp, i));
        return result;
    }


    @Test(dataProvider = "events")
    public void eventEntity(TestEventToStore event) throws CradleStorageException, IOException, DataFormatException, CradleIdException, CompressException {
        SerializedEntity<SerializedEntityMetadata, TestEventEntity> serializedEntity = TestEventEntityUtils.toSerializedEntity(event, page, CompressionType.ZLIB, 2000);
        TestEventEntity entity = serializedEntity.getEntity();
        StoredTestEvent newEvent = TestEventEntityUtils.toStoredTestEvent(entity, page);

        RecursiveComparisonConfiguration config = new RecursiveComparisonConfiguration();
        config.ignoreFieldsMatchingRegexes("pageId", ".*\\.pageId", "error", ".*\\.error", "recDate", ".*\\.recDate", "lastStartTimestamp", ".*\\.lastStartTimestamp");
config.ignoreAllOverriddenEquals();
        Assertions.assertThat(newEvent)
                .usingRecursiveComparison(config)
                .isEqualTo(event);
    }
}
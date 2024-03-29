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

package com.exactpro.cradle.testevents;

import com.exactpro.cradle.BookId;
import com.exactpro.cradle.BookInfo;
import com.exactpro.cradle.CoreStorageSettings;
import com.exactpro.cradle.Direction;
import com.exactpro.cradle.PageId;
import com.exactpro.cradle.PageInfo;
import com.exactpro.cradle.TestPageLoader;
import com.exactpro.cradle.TestPagesLoader;
import com.exactpro.cradle.TestUtils;
import com.exactpro.cradle.messages.StoredMessageId;
import com.exactpro.cradle.serialization.EventsSizeCalculator;
import com.exactpro.cradle.utils.CradleStorageException;
import com.exactpro.cradle.utils.TestEventUtils;
import org.assertj.core.api.Assertions;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import org.testng.asserts.SoftAssert;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;

import static com.exactpro.cradle.testevents.EventSingleTest.BOOK;
import static com.exactpro.cradle.testevents.EventSingleTest.DUMMY_ID;
import static com.exactpro.cradle.testevents.EventSingleTest.DUMMY_NAME;
import static com.exactpro.cradle.testevents.EventSingleTest.ID_VALUE;
import static com.exactpro.cradle.testevents.EventSingleTest.SCOPE;
import static com.exactpro.cradle.testevents.EventSingleTest.START_TIMESTAMP;
import static com.exactpro.cradle.testevents.EventSingleTest.batchParentId;
import static com.exactpro.cradle.testevents.EventSingleTest.validEvent;
import static java.time.temporal.ChronoUnit.NANOS;

public class EventBatchTest {
    private final int MAX_SIZE = 1024;

    private final long storeActionRejectionThreshold = new CoreStorageSettings().calculateStoreActionRejectionThreshold();

    private final TestEventSingleToStoreBuilder eventBuilder = new TestEventSingleToStoreBuilder(storeActionRejectionThreshold);
    private final TestEventBatchToStoreBuilder batchBuilder = new TestEventBatchToStoreBuilder(MAX_SIZE, storeActionRejectionThreshold);
    private final StoredTestEventId batchId = new StoredTestEventId(BOOK, SCOPE, START_TIMESTAMP, UUID.randomUUID().toString());
    private TestEventBatchToStore batch;

    @BeforeMethod
    public void prepareBatch() throws CradleStorageException {
        batch = batchBuilder
                .id(batchId)
                .parentId(batchParentId)
                .build();
    }

    @DataProvider(name = "batch invalid events")
    public Object[][] batchInvalidEvents() {
        Object[][] batchEvents = new Object[][]
                {
                        {validEvent().parentId(null),                                                                      //No parent ID
                                "must have a parent"},
                        {validEvent().id(new BookId(BOOK.getName() + "1"), SCOPE, START_TIMESTAMP, DUMMY_NAME)               //Different book
                                .parentId(new StoredTestEventId(new BookId(BOOK.getName() + "1"),
                                SCOPE, START_TIMESTAMP, "Parent_" + DUMMY_NAME)),
                                "events of book"},
                        {validEvent().id(BOOK, SCOPE + "1", START_TIMESTAMP, DUMMY_NAME),                                    //Different scope
                                "events of scope"},
                        {validEvent().id(BOOK, SCOPE, START_TIMESTAMP.minusMillis(5000), DUMMY_NAME),                      //Early timestamp
                                "start timestamp"},
                        {validEvent().parentId(new StoredTestEventId(BOOK, SCOPE, START_TIMESTAMP, "wrong_parent")),       //Different parent ID
                                "parent"},
                        {validEvent().parentId(null),                                                                      //No parent
                                "must have a parent"}
                };

        return Stream.of(new EventSingleTest().invalidEvents(), batchEvents)
                .flatMap(Arrays::stream)
                .toArray(Object[][]::new);
    }


    @Test
    public void batchFields() throws CradleStorageException {
        TestEventBatchToStore event = new TestEventBatchToStoreBuilder(MAX_SIZE, storeActionRejectionThreshold)
                .id(DUMMY_ID)
                .name("Name1")
                .parentId(batchParentId)
                .type("Type1")
                .build();

        Set<StoredMessageId> messages1 = Collections.singleton(new StoredMessageId(BOOK, "Session1", Direction.FIRST, Instant.EPOCH, 1));
        event.addTestEvent(eventBuilder.id(BOOK, SCOPE, START_TIMESTAMP.plusMillis(3500), ID_VALUE)
                .name(DUMMY_NAME)
                .parentId(batch.getParentId())
                .endTimestamp(START_TIMESTAMP.plusMillis(5000))
                .messages(messages1)
                .success(true)
                .build());

        Set<StoredMessageId> messages2 = Collections.singleton(new StoredMessageId(BOOK, "Session2", Direction.SECOND, Instant.EPOCH, 2));
        event.addTestEvent(eventBuilder.id(BOOK, SCOPE, START_TIMESTAMP, ID_VALUE + "1")
                .name(DUMMY_NAME)
                .parentId(batch.getParentId())
                .messages(messages2)
                .success(false)
                .build());

        event.addTestEvent(eventBuilder.id(BOOK, SCOPE, START_TIMESTAMP, ID_VALUE + "2")
                .name(DUMMY_NAME)
                .parentId(batch.getParentId())
                .success(false)
                .build());

        StoredTestEventBatch stored = new StoredTestEventBatch(event, null);

        EventTestUtils.assertEvents(stored, event);
    }

    @Test
    public void passedBatchEvent() throws CradleStorageException {
        StoredTestEventId batchId = new StoredTestEventId(BOOK, SCOPE, Instant.EPOCH, "BatchID"),
                parentId = new StoredTestEventId(BOOK, SCOPE, Instant.EPOCH, "ParentID");
        TestEventSingleToStore event = eventBuilder
                .id(DUMMY_ID)
                .name(DUMMY_NAME)
                .parentId(parentId)
                .content("Test content".getBytes())
                .build();

        TestEventBatchToStore batch = new TestEventBatchToStore(batchId, null, parentId, MAX_SIZE, storeActionRejectionThreshold);
        batch.addTestEvent(event);
        TestEventUtils.validateTestEvent(batch, storeActionRejectionThreshold);
    }

    @Test(expectedExceptions = {CradleStorageException.class}, expectedExceptionsMessageRegExp = "Batch must have a parent")
    public void batchParentMustBeSet() throws CradleStorageException {
        batchBuilder.id(DUMMY_ID).build();
    }

    @Test(expectedExceptions = {CradleStorageException.class}, expectedExceptionsMessageRegExp = "Batch has not enough space to hold given test event")
    public void batchContentIsLimited() throws CradleStorageException {
        byte[] content = new byte[5000];
        for (int i = 0; i <= (MAX_SIZE / content.length) + 1; i++)
            batch.addTestEvent(eventBuilder
                    .id(new StoredTestEventId(BOOK, SCOPE, Instant.EPOCH, Integer.toString(i)))
                    .name(DUMMY_NAME)
                    .parentId(batch.getParentId())
                    .content(content)
                    .build());
    }

    @Test
    public void batchCountsSpaceLeft() throws CradleStorageException {
        byte[] content = new byte[MAX_SIZE / 2];
        long left = batch.getSpaceLeft();

        TestEventSingleToStore event = eventBuilder
                .id(new StoredTestEventId(BOOK, SCOPE, START_TIMESTAMP, "1"))
                .name(DUMMY_NAME)
                .parentId(batch.getParentId())
                .content(content)
                .build();

        batch.addTestEvent(event);

        Assert.assertEquals(batch.getSpaceLeft(), left - EventsSizeCalculator.calculateRecordSizeInBatch(event), "Batch counts space left");
    }

    @Test
    public void batchChecksSpaceLeft() throws CradleStorageException {
        byte[] content = new byte[MAX_SIZE / 2];

        TestEventSingleToStore event = eventBuilder
                .id(new StoredTestEventId(BOOK, SCOPE, START_TIMESTAMP, "1"))
                .name(DUMMY_NAME)
                .parentId(batch.getParentId())
                .content(content)
                .build();
        batch.addTestEvent(event);
        Assert.assertFalse(batch.hasSpace(event), "Batch shows if it has space to hold given test event");
    }

    @Test(expectedExceptions = {CradleStorageException.class}, expectedExceptionsMessageRegExp = "Test event with ID .* is already present in batch")
    public void duplicateIds() throws CradleStorageException {
        StoredTestEventId eventId = new StoredTestEventId(BOOK, SCOPE, START_TIMESTAMP, "AAA");
        batch.addTestEvent(eventBuilder.id(eventId).name(DUMMY_NAME).parentId(batch.getParentId()).build());
        batch.addTestEvent(eventBuilder.id(eventId).name(DUMMY_NAME).parentId(batch.getParentId()).build());
    }

    @Test(expectedExceptions = {CradleStorageException.class}, expectedExceptionsMessageRegExp = ".* '.*:XXX' .* stored in this batch .*")
    public void externalReferences() throws CradleStorageException {
        StoredTestEventId parentId = new StoredTestEventId(BOOK, SCOPE, START_TIMESTAMP, "1");
        batch.addTestEvent(eventBuilder.id(parentId)
                .name(DUMMY_NAME)
                .parentId(batch.getParentId())
                .build());
        batch.addTestEvent(eventBuilder.id(BOOK, SCOPE, START_TIMESTAMP, "2")
                .name(DUMMY_NAME)
                .parentId(batch.getParentId())
                .build());
        batch.addTestEvent(eventBuilder.id(BOOK, SCOPE, START_TIMESTAMP, "3")
                .name(DUMMY_NAME)
                .parentId(parentId)
                .build());
        batch.addTestEvent(eventBuilder.id(BOOK, SCOPE, START_TIMESTAMP, "4")
                .name(DUMMY_NAME)
                .parentId(new StoredTestEventId(BOOK, SCOPE, START_TIMESTAMP, "XXX"))
                .build());
    }

    @Test(expectedExceptions = {CradleStorageException.class}, expectedExceptionsMessageRegExp = ".* stored in this batch .*")
    public void referenceToBatch() throws CradleStorageException {
        StoredTestEventId parentId = new StoredTestEventId(BOOK, SCOPE, START_TIMESTAMP, "1");
        batch.addTestEvent(eventBuilder.id(parentId)
                .name(DUMMY_NAME)
                .parentId(batch.getParentId())
                .build());
        batch.addTestEvent(eventBuilder.id(BOOK, SCOPE, START_TIMESTAMP, "2")
                .name(DUMMY_NAME)
                .parentId(batch.getId())
                .build());
    }

    @Test
    public void childrenAligned() throws CradleStorageException {
        BatchedStoredTestEvent parentEvent = batch.addTestEvent(eventBuilder
                .id(BOOK, SCOPE, START_TIMESTAMP, "1")
                .name(DUMMY_NAME)
                .parentId(batch.getParentId())
                .build()),
                childEvent = batch.addTestEvent(eventBuilder
                        .id(BOOK, SCOPE, START_TIMESTAMP, "2")
                        .name(DUMMY_NAME)
                        .parentId(parentEvent.getId())
                        .build());

        Assert.assertTrue(parentEvent.getChildren().contains(childEvent), "Children are aligned with their parent");
    }

    @Test
    public void rootIsRoot() throws CradleStorageException {
        BatchedStoredTestEvent parentEvent = batch.addTestEvent(eventBuilder
                .id(DUMMY_ID)
                .name(DUMMY_NAME)
                .parentId(batch.getParentId())
                .build());
        Assert.assertTrue(batch.getRootTestEvents().contains(parentEvent), "Root event is listed in roots");
    }

    @Test
    public void childIsNotRoot() throws CradleStorageException {
        BatchedStoredTestEvent parentEvent = batch.addTestEvent(eventBuilder
                .id(BOOK, SCOPE, START_TIMESTAMP, "1")
                .name(DUMMY_NAME)
                .parentId(batch.getParentId())
                .build()),
                childEvent = batch.addTestEvent(eventBuilder
                        .id(BOOK, SCOPE, START_TIMESTAMP, "2")
                        .name(DUMMY_NAME)
                        .parentId(parentEvent.getId())
                        .build());

        Assert.assertFalse(batch.getRootTestEvents().contains(childEvent), "Child event is not listed in roots");
    }

    @Test(dataProvider = "batch invalid events",
            expectedExceptions = {CradleStorageException.class})
    public void batchEventValidation(TestEventSingleToStoreBuilder builder, String errorMessage) throws CradleStorageException {
        try {
            var singleEvent = builder.build();
            BookInfo bookInfo = createBookInfo();
            TestEventUtils.validateTestEvent(singleEvent, bookInfo, storeActionRejectionThreshold);
            batch.addTestEvent(singleEvent);
            Assertions.fail("Invalid message passed validation");
        } catch (CradleStorageException e) {
            TestUtils.handleException(e, errorMessage);
        }
    }

    private static BookInfo createBookInfo() {
        List<PageInfo> pages = List.of(new PageInfo(
                new PageId(BOOK, START_TIMESTAMP, "test-page"),
                START_TIMESTAMP.plus(1, NANOS),
                null)
        );
        return new BookInfo(
                BOOK,
                null,
                null,
                START_TIMESTAMP,
                1,
                Long.MAX_VALUE,
                new TestPagesLoader(pages),
                new TestPageLoader(pages, true), new TestPageLoader(pages, false));
    }

    @Test
    public void batchEventMessagesAreIndependent() throws CradleStorageException {
        TestEventSingleToStore event = validEvent().success(true).message(new StoredMessageId(BOOK, "Session1", Direction.FIRST, START_TIMESTAMP, 1)).build();
        BatchedStoredTestEvent stored = batch.addTestEvent(event);

        StoredMessageId newMessage = new StoredMessageId(BOOK, "Session2", Direction.SECOND, START_TIMESTAMP, 2);
        event.getMessages().add(newMessage);

        Assert.assertFalse(stored.getMessages().contains(newMessage), "messages in batched event contain new message");
    }

    @Test
    public void storedBatchIsIndependent() throws CradleStorageException {
        Instant end = START_TIMESTAMP.plusMillis(5000);
        TestEventSingleToStore event = validEvent()
                .success(true)
                .endTimestamp(end)
                .message(new StoredMessageId(BOOK, "Session1", Direction.FIRST, START_TIMESTAMP, 1))
                .build();
        batch.addTestEvent(event);
        StoredTestEventBatch stored = StoredTestEvent.batch(batch, null);

        StoredMessageId newMessage = new StoredMessageId(BOOK, "Session2", Direction.SECOND, START_TIMESTAMP, 2);
        event.getMessages().add(newMessage);

        StoredMessageId newMessage2 = new StoredMessageId(BOOK, "Session3", Direction.FIRST, START_TIMESTAMP, 3);
        TestEventSingleToStore event2 = validEvent()
                .id(BOOK, SCOPE, START_TIMESTAMP, ID_VALUE + "_2")
                .success(false)
                .endTimestamp(end.plusMillis(1000))
                .message(newMessage2)
                .build();
        batch.addTestEvent(event2);

        SoftAssert soft = new SoftAssert();
        soft.assertNull(stored.getTestEvent(event2.getId()), "event added to batch after storing");
        soft.assertFalse(stored.getMessages().contains(newMessage), "messages in stored event contain new message");
        soft.assertFalse(stored.getMessages().contains(newMessage2), "messages in stored event contain new message 2");
        soft.assertTrue(stored.isSuccess());
        soft.assertEquals(stored.getEndTimestamp(), end);
        soft.assertAll();
    }
}

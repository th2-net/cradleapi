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
import com.exactpro.cradle.utils.CradleStorageException;
import com.exactpro.cradle.utils.TestEventUtils;
import org.assertj.core.api.Assertions;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class EventSingleTest {
    public static final BookId BOOK = new BookId("book1");
    public static final String SCOPE = "default",
            DUMMY_NAME = "TestEvent",
            ID_VALUE = "Event_ID";
    public static final Instant START_TIMESTAMP = Instant.now().minus(10, ChronoUnit.SECONDS);
    public static final Instant BEFORE_START_TIMESTAMP = START_TIMESTAMP.minus(10, ChronoUnit.SECONDS);
    public static final StoredTestEventId DUMMY_ID = new StoredTestEventId(BOOK, SCOPE, START_TIMESTAMP, ID_VALUE),
            batchParentId = new StoredTestEventId(BOOK, SCOPE, START_TIMESTAMP, "BatchParentID");

    private final long storeActionRejectionThreshold = new CoreStorageSettings().calculateStoreActionRejectionThreshold();

    private final TestEventSingleToStoreBuilder eventBuilder = new TestEventSingleToStoreBuilder(storeActionRejectionThreshold);

    @DataProvider(name = "invalid events")
    public Object[][] invalidEvents() {
        return new Object[][]
                {
                        {new TestEventSingleToStoreBuilder(storeActionRejectionThreshold),                                                             //Empty event
                                "ID cannot be null"},
                        {validEvent().id(null),                                                                           //No ID
                                "ID cannot be null"},
                        {validEvent().id(null, SCOPE, START_TIMESTAMP, ID_VALUE),                                         //No book
                                "must have a book"},
                        {validEvent().id(BOOK, null, START_TIMESTAMP, ID_VALUE),                                          //No scope
                                "must have a scope"},
                        {validEvent().id(BOOK, SCOPE, null, ID_VALUE),                                                    //No timestamp
                                "must have a start timestamp"},
                        {validEvent().name(null),                                                                         //No name
                                "must have a name"},
                        {validEvent().parentId(DUMMY_ID),                                                                 //Self-reference
                                "reference itself"},
                        {validEvent().endTimestamp(START_TIMESTAMP.minusMillis(5000)),                                    //End before start
                                "cannot end sooner than it started"},
                        {validEvent().messages(Collections.singleton(new StoredMessageId(new BookId(BOOK.getName() + "1"),  //Different book in message
                                "Session1", Direction.FIRST, START_TIMESTAMP, 1))), "Book of message"},
                        {validEvent().parentId(new StoredTestEventId(BOOK, SCOPE, BEFORE_START_TIMESTAMP, ID_VALUE)),
                                "could not find corresponding page in book"}
                };
    }


    public static TestEventSingleToStoreBuilder validEvent() {
        //Preparing valid event that corresponds to the batch. It will be made invalid in "invalid events"
        return new TestEventSingleToStoreBuilder(new CoreStorageSettings().calculateStoreActionRejectionThreshold())
                .id(DUMMY_ID)
                .parentId(batchParentId)
                .name(DUMMY_NAME);
    }


    @Test
    public void eventFields() throws CradleStorageException {
        Set<StoredMessageId> messages = new HashSet<>();
        messages.add(new StoredMessageId(BOOK, "Session1", Direction.FIRST, Instant.EPOCH, 1));
        messages.add(new StoredMessageId(BOOK, "Session2", Direction.SECOND, Instant.EPOCH, 2));

        TestEventSingleToStore event = eventBuilder
                .id(DUMMY_ID)
                .name("Name1")
                .parentId(batchParentId)
                .type("Type1")
                .success(true)
                .endTimestamp(Instant.now())
                .messages(messages)
                .content("Valid content".getBytes())
                .build();
        StoredTestEventSingle stored = new StoredTestEventSingle(event, null);

        EventTestUtils.assertEvents(stored, event);
    }

    @Test(dataProvider = "invalid events",
            expectedExceptions = {CradleStorageException.class})
    public void eventValidation(TestEventSingleToStoreBuilder builder, String errorMessage) throws CradleStorageException {
        try {
            BookInfo bookInfo = createBookInfo();
            TestEventUtils.validateTestEvent(builder.build(), bookInfo, storeActionRejectionThreshold);
            Assertions.fail("Invalid message passed validation");
        } catch (CradleStorageException e) {
            TestUtils.handleException(e, errorMessage);
        }
    }

    private static BookInfo createBookInfo() {
        List<PageInfo> pages = List.of(new PageInfo(
                new PageId(BOOK, START_TIMESTAMP, "test-page"),
                START_TIMESTAMP,
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
    public void passedEvent() throws CradleStorageException {
        TestEventSingleToStore event = eventBuilder
                .id(DUMMY_ID)
                .name(DUMMY_NAME)
                .content("Test content".getBytes())
                .build();
        TestEventUtils.validateTestEvent(event, storeActionRejectionThreshold);
    }

    @Test
    public void storedEventMessagesAreIndependent() throws CradleStorageException {
        TestEventSingleToStore event = validEvent().message(new StoredMessageId(BOOK, "Session1", Direction.FIRST, START_TIMESTAMP, 1))
                .message(new StoredMessageId(BOOK, "Session2", Direction.SECOND, START_TIMESTAMP, 2))
                .build();
        StoredTestEvent stored = StoredTestEvent.single(event, null);

        StoredMessageId newMessage = new StoredMessageId(BOOK, "Session3", Direction.FIRST, START_TIMESTAMP, 3);
        event.getMessages().add(newMessage);
        Assert.assertFalse(stored.getMessages().contains(newMessage), "messages in stored event contain new message");
    }
}

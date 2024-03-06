/*
 * Copyright 2023-2024 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.cradle.cassandra.integration;

import com.exactpro.cradle.BookToAdd;
import com.exactpro.cradle.Direction;
import com.exactpro.cradle.counters.Interval;
import com.exactpro.cradle.messages.GroupedMessageFilter;
import com.exactpro.cradle.messages.GroupedMessageFilterBuilder;
import com.exactpro.cradle.messages.MessageFilter;
import com.exactpro.cradle.messages.MessageFilterBuilder;
import com.exactpro.cradle.testevents.TestEventFilter;
import com.exactpro.cradle.testevents.TestEventFilterBuilder;
import com.exactpro.cradle.utils.CradleStorageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.time.Instant;

import static com.google.common.collect.Lists.newArrayList;
import static java.util.Collections.emptyList;
import static org.testng.Assert.assertEquals;

public class BookWithoutPageTest extends BaseCradleCassandraTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(BookWithoutPageTest.class);

    @BeforeClass
    public void startUp() throws IOException, InterruptedException, CradleStorageException {
        super.startUp(false);
        generateData();
    }

    @Override
    protected void generateData() throws CradleStorageException, IOException {
        try {
            storage.addBook(new BookToAdd(bookId.getName()));
        } catch (CradleStorageException | IOException e) {
            LOGGER.error("Error while generating data:", e);
            throw e;
        }
    }

    @Test
    public void testGetAllPages() throws CradleStorageException {
        try {
            assertEquals(storage.getAllPages(bookId), emptyList());
        } catch (CradleStorageException e) {
            LOGGER.error(e.getMessage(), e);
            throw e;
        }
    }

    @Test
    public void testGetMessages() throws CradleStorageException, IOException {
        try {
            MessageFilter filter = new MessageFilterBuilder()
                    .bookId(bookId)
                    .sessionAlias("test-session-alias")
                    .direction(Direction.FIRST)
                    .timestampFrom().isGreaterThan(Instant.MIN)
                    .timestampTo().isLessThan(Instant.MAX)
                    .build();
            assertEquals(newArrayList(storage.getMessages(filter)), emptyList());
        } catch (CradleStorageException | IOException e) {
            LOGGER.error(e.getMessage(), e);
            throw e;
        }
    }

    @Test
    public void testGetMessageBatches() throws CradleStorageException, IOException {
        try {
            MessageFilter filter = new MessageFilterBuilder()
                    .bookId(bookId)
                    .sessionAlias("test-session-alias")
                    .direction(Direction.FIRST)
                    .timestampFrom().isGreaterThan(Instant.MIN)
                    .timestampTo().isLessThan(Instant.MAX)
                    .build();
            assertEquals(newArrayList(storage.getMessageBatches(filter)), emptyList());
        } catch (CradleStorageException | IOException e) {
            LOGGER.error(e.getMessage(), e);
            throw e;
        }
    }

    @Test
    public void testGetGroupedMessageBatches() throws CradleStorageException, IOException {
        try {
            GroupedMessageFilter filter = new GroupedMessageFilterBuilder()
                    .bookId(bookId)
                    .groupName("test-group")
                    .timestampFrom().isGreaterThan(Instant.MIN)
                    .timestampTo().isLessThan(Instant.MAX)
                    .build();
            assertEquals(newArrayList(storage.getGroupedMessageBatches(filter)), emptyList());
        } catch (CradleStorageException | IOException e) {
            LOGGER.error(e.getMessage(), e);
            throw e;
        }
    }

    @Test
    public void testGetLastSequence() throws CradleStorageException, IOException {
        try {
            assertEquals(storage.getLastSequence("test-session-alias", Direction.FIRST, bookId), -1L);
        } catch (CradleStorageException | IOException e) {
            LOGGER.error(e.getMessage(), e);
            throw e;
        }
    }

    @Test
    public void testGetFirstSequence() throws CradleStorageException, IOException {
        try {
            assertEquals(storage.getFirstSequence("test-session-alias", Direction.FIRST, bookId), -1L);
        } catch (CradleStorageException | IOException e) {
            LOGGER.error(e.getMessage(), e);
            throw e;
        }
    }

    @Test
    public void testGetSessionAliases1() throws CradleStorageException, IOException {
        try {
            assertEquals(storage.getSessionAliases(bookId), emptyList());
        } catch (CradleStorageException | IOException e) {
            LOGGER.error(e.getMessage(), e);
            throw e;
        }
    }

    @Test
    public void testGetSessionAliases2() throws CradleStorageException {
        try {
            assertEquals(newArrayList(storage.getSessionAliases(bookId, new Interval(Instant.MIN, Instant.MAX))),
                    emptyList());
        } catch (CradleStorageException e) {
            LOGGER.error(e.getMessage(), e);
            throw e;
        }
    }

    @Test
    public void testGetGroups() throws CradleStorageException, IOException {
        try {
            assertEquals(storage.getGroups(bookId), emptyList());
        } catch (CradleStorageException | IOException e) {
            LOGGER.error(e.getMessage(), e);
            throw e;
        }
    }

    @Test
    public void testGetTestEvents() throws CradleStorageException, IOException {
        try {
            TestEventFilter filter = new TestEventFilterBuilder()
                    .bookId(bookId)
                    .scope("test-scope")
                    .timestampFrom().isGreaterThan(Instant.MIN)
                    .timestampTo().isLessThan(Instant.MAX)
                    .build();
            assertEquals(newArrayList(storage.getTestEvents(filter)), emptyList());
        } catch (CradleStorageException | IOException e) {
            LOGGER.error(e.getMessage(), e);
            throw e;
        }
    }

    @Test
    public void testGetScopes1() throws CradleStorageException, IOException {
        try {
            assertEquals(storage.getScopes(bookId), emptyList());
        } catch (CradleStorageException | IOException e) {
            LOGGER.error(e.getMessage(), e);
            throw e;
        }
    }

    @Test
    public void testGetScopes2() throws CradleStorageException {
        try {
            assertEquals(newArrayList(storage.getScopes(bookId, new Interval(Instant.MIN, Instant.MAX))),
                    emptyList());
        } catch (CradleStorageException e) {
            LOGGER.error(e.getMessage(), e);
            throw e;
        }
    }

    @Test
    public void testGetSessionGroups() throws CradleStorageException {
        try {
            assertEquals(newArrayList(storage.getSessionGroups(bookId, new Interval(Instant.MIN, Instant.MAX))),
                    emptyList());
        } catch (CradleStorageException e) {
            LOGGER.error(e.getMessage(), e);
            throw e;
        }
    }

    @Test
    public void testGetPages() throws CradleStorageException {
        try {
            assertEquals(newArrayList(storage.getPages(bookId, new Interval(Instant.MIN, Instant.MAX))),
                    emptyList());
        } catch (CradleStorageException e) {
            LOGGER.error(e.getMessage(), e);
            throw e;
        }
    }
}

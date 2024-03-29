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

package com.exactpro.cradle.cassandra.dao.messages;

import com.exactpro.cradle.BookId;
import com.exactpro.cradle.BookInfo;
import com.exactpro.cradle.Direction;
import com.exactpro.cradle.cassandra.dao.CassandraOperators;
import com.exactpro.cradle.messages.MessageFilter;
import com.exactpro.cradle.utils.CradleStorageException;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import java.time.Instant;
import java.util.Collections;

public class MessagesIteratorProviderTest {
    protected static final BookId DEFAULT_BOOK_ID = new BookId("test_book");
    private static final String FIRST_SESSION_ALIAS = "test_session_alias";

    @Test(description = "Create iterator provider with empty book")
    public void createIteratorProviderWithEmptyBook() throws CradleStorageException {
        CassandraOperators operators = Mockito.mock(CassandraOperators.class);
        MessageFilter messageFilter = new MessageFilter(DEFAULT_BOOK_ID, FIRST_SESSION_ALIAS, Direction.FIRST, null);
        new MessagesIteratorProvider(
                "",
                messageFilter,
                operators,
                new BookInfo(DEFAULT_BOOK_ID,
                        "book_name",
                        "",
                        Instant.now(),
                        1,
                        Long.MAX_VALUE,
                        (bookId, start, end) -> Collections.emptyList(),
                        bookId -> null,
                        bookId -> null),
                null,
                null,
                null
        );
    }
}
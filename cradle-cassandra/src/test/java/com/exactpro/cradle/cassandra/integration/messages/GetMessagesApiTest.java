/*
 * Copyright 2023 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.cradle.cassandra.integration.messages;

import com.exactpro.cradle.Direction;
import com.exactpro.cradle.messages.MessageFilter;
import com.exactpro.cradle.messages.MessageFilterBuilder;
import com.exactpro.cradle.utils.CradleStorageException;
import org.assertj.core.api.Assertions;
import org.assertj.core.util.Lists;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.time.temporal.ChronoUnit;

public class GetMessagesApiTest extends BaseMessageApiTest {
    @BeforeClass
    public void startUp() throws IOException, InterruptedException, CradleStorageException {
        super.startUp(true);
        generateData();
    }

    @Test(description = "Get messages withing interval with start time less than batch start time and end time more then batch end time")
    public void getMessagesWithWideIntervalTest() throws CradleStorageException, IOException {
        MessageFilter filter = new MessageFilterBuilder()
                .bookId(bookId)
                .sessionAlias(SESSION_ALIAS5)
                .direction(Direction.SECOND)
                .timestampFrom().isGreaterThanOrEqualTo(dataStart.plus(24, ChronoUnit.MINUTES))
                .timestampTo().isLessThan(dataStart.plus(30, ChronoUnit.MINUTES))
                .build();
        var actual = storage.getMessages(filter);
        var resultAsList = Lists.newArrayList(actual.asIterable());
            Assertions.assertThat(resultAsList.size()).isEqualTo(2);
    }

    @Test(description = "Get messages withing interval with start time more than batch start time and end time more then batch end time")
    public void getMessagesWithHalfWideIntervalTest() throws CradleStorageException, IOException {
        MessageFilter filter = new MessageFilterBuilder()
                .bookId(bookId)
                .sessionAlias(SESSION_ALIAS5)
                .direction(Direction.SECOND)
                .timestampFrom().isGreaterThanOrEqualTo(dataStart.plus(26, ChronoUnit.MINUTES))
                .timestampTo().isLessThan(dataStart.plus(30, ChronoUnit.MINUTES))
                .build();
        var actual = storage.getMessages(filter);
        var resultAsList = Lists.newArrayList(actual.asIterable());
        Assertions.assertThat(resultAsList.size()).isEqualTo(2);
    }


    @Test(description = "Get messages withing interval with start time more than batch start time and end time more then batch end time")
    public void getMessagesWithHalfWideIntervalTest2() throws CradleStorageException, IOException {
        MessageFilter filter = new MessageFilterBuilder()
                .bookId(bookId)
                .sessionAlias(SESSION_ALIAS5)
                .direction(Direction.SECOND)
                .timestampFrom().isGreaterThanOrEqualTo(dataStart.plus(27, ChronoUnit.MINUTES))
                .timestampTo().isLessThan(dataStart.plus(30, ChronoUnit.MINUTES))
                .build();
        var actual = storage.getMessages(filter);
        var resultAsList = Lists.newArrayList(actual.asIterable());
        Assertions.assertThat(resultAsList.size()).isEqualTo(1);
    }

    @Test(description = "Get messages withing interval with start time more than batch start time and end time less then batch end time")
    public void getMessagesWithNarrowIntervalTest() throws CradleStorageException, IOException {
        MessageFilter filter = new MessageFilterBuilder()
                .bookId(bookId)
                .sessionAlias(SESSION_ALIAS5)
                .direction(Direction.SECOND)
                .timestampFrom().isGreaterThanOrEqualTo(dataStart.plus(26, ChronoUnit.MINUTES))
                .timestampTo().isLessThan(dataStart.plus(29, ChronoUnit.MINUTES))
                .build();
        var actual = storage.getMessages(filter);
        var resultAsList = Lists.newArrayList(actual.asIterable());
        Assertions.assertThat(resultAsList.size()).isEqualTo(2);
    }

    @Test(description = "Get messages withing interval with start time more than batch start time and end time less then batch end time")
    public void getMessagesWithNarrowIntervalTest2() throws CradleStorageException, IOException {
        MessageFilter filter = new MessageFilterBuilder()
                .bookId(bookId)
                .sessionAlias(SESSION_ALIAS5)
                .direction(Direction.SECOND)
                .timestampFrom().isGreaterThanOrEqualTo(dataStart.plus(26, ChronoUnit.MINUTES))
                .timestampTo().isLessThan(dataStart.plus(28, ChronoUnit.MINUTES))
                .build();
        var actual = storage.getMessages(filter);
        var resultAsList = Lists.newArrayList(actual.asIterable());
        Assertions.assertThat(resultAsList.size()).isEqualTo(1);
    }

    @Test(description = "Get messages withing interval with start time more than batch start time and end time less then batch end time")
    public void getMessagesWithNarrowIntervalTest3() throws CradleStorageException, IOException {
        MessageFilter filter = new MessageFilterBuilder()
                .bookId(bookId)
                .sessionAlias(SESSION_ALIAS5)
                .direction(Direction.SECOND)
                .timestampFrom().isGreaterThanOrEqualTo(dataStart.plus(26, ChronoUnit.MINUTES))
                .timestampTo().isLessThan(dataStart.plus(27, ChronoUnit.MINUTES))
                .build();
        var actual = storage.getMessages(filter);
        var resultAsList = Lists.newArrayList(actual.asIterable());
        Assertions.assertThat(resultAsList.size()).isEqualTo(1);
    }
}

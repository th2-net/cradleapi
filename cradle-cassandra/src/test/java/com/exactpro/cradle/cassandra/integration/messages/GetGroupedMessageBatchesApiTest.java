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

import com.exactpro.cradle.messages.GroupedMessageFilter;
import com.exactpro.cradle.utils.CradleStorageException;
import org.assertj.core.api.Assertions;
import org.assertj.core.util.Lists;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.time.temporal.ChronoUnit;

public class GetGroupedMessageBatchesApiTest extends BaseMessageApiTest {
    @BeforeClass
    public void startUp() throws IOException, InterruptedException, CradleStorageException {
        super.startUp(true);
        generateData();
    }

    @Test(description = "Get grouped messages within 1 page. Interval start time is less than batch start time and end time more then batch end time")
    public void getGroupedMessagesWithWideIntervalTest() throws CradleStorageException, IOException {
        GroupedMessageFilter filter = GroupedMessageFilter.builder()
                .groupName(GROUP3_NAME)
                .bookId(bookId)
                .timestampFrom().isGreaterThanOrEqualTo(dataStart.plus(24, ChronoUnit.MINUTES))
                .timestampTo().isLessThan(dataStart.plus(30, ChronoUnit.MINUTES))
                .build();
        var actual = storage.getGroupedMessageBatches(filter);
        var resultAsList = Lists.newArrayList(actual.asIterable());
        Assertions.assertThat(resultAsList.size()).isEqualTo(1);
        Assertions.assertThat(resultAsList.get(0).getMessages().size()).isEqualTo(4);
    }

    @Test(description = "Get grouped messages within 1 page. Interval start time is equal to batch end time.")
    public void getGroupedMessagesWithIntervalAtEnd() throws CradleStorageException, IOException {
        GroupedMessageFilter filter = GroupedMessageFilter.builder()
                .groupName(GROUP3_NAME)
                .bookId(bookId)
                .timestampFrom().isGreaterThanOrEqualTo(dataStart.plus(29, ChronoUnit.MINUTES))
                .timestampTo().isLessThan(dataStart.plus(30, ChronoUnit.MINUTES))
                .build();
        var actual = storage.getGroupedMessageBatches(filter);
        var resultAsList = Lists.newArrayList(actual.asIterable());
        Assertions.assertThat(resultAsList.size()).isEqualTo(1);
        Assertions.assertThat(resultAsList.get(0).getMessages().size()).isEqualTo(4);
    }
    @Test(description = "Get grouped messages withing interval that covers multiple pages. First batch start time is equal to interval start and interval end time is after last batch end.")
    public void getGroupedMessagesWithWideIntervalTest2() throws CradleStorageException, IOException {
        GroupedMessageFilter filter = GroupedMessageFilter.builder()
                .groupName(GROUP3_NAME)
                .bookId(bookId)
                .timestampFrom().isGreaterThanOrEqualTo(dataStart.plus(25, ChronoUnit.MINUTES))
                .timestampTo().isLessThan(dataStart.plus(58, ChronoUnit.MINUTES))
                .build();
        var actual = storage.getGroupedMessageBatches(filter);
        var resultAsList = Lists.newArrayList(actual.asIterable());
        Assertions.assertThat(resultAsList.size()).isEqualTo(4);
        Assertions.assertThat(resultAsList.get(0).getMessages().size()).isEqualTo(4);
        Assertions.assertThat(resultAsList.get(1).getMessages().size()).isEqualTo(1);
        Assertions.assertThat(resultAsList.get(2).getMessages().size()).isEqualTo(1);
        Assertions.assertThat(resultAsList.get(3).getMessages().size()).isEqualTo(2);
    }


    @Test(description = "Get grouped messages withing interval that covers multiple pages. Interval start time is in the middle of the first batch and interval end is before the end of the last batch.")
    public void getGroupedMessagesWithWideIntervalTest4() throws CradleStorageException, IOException {
        GroupedMessageFilter filter = GroupedMessageFilter.builder()
                .groupName(GROUP3_NAME)
                .bookId(bookId)
                .timestampFrom().isGreaterThanOrEqualTo(dataStart.plus(27, ChronoUnit.MINUTES))
                .timestampTo().isLessThan(dataStart.plus(46, ChronoUnit.MINUTES))
                .build();
        var actual = storage.getGroupedMessageBatches(filter);
        var resultAsList = Lists.newArrayList(actual.asIterable());
        Assertions.assertThat(resultAsList.size()).isEqualTo(2);
        Assertions.assertThat(resultAsList.get(0).getMessages().size()).isEqualTo(4);
        Assertions.assertThat(resultAsList.get(1).getMessages().size()).isEqualTo(1);
    }

    @Test(description = "Get grouped messages withing interval with start time less than batch start time and end time less then batch end time")
    public void getGroupedMessagesWithHalfWideIntervalTest0() throws CradleStorageException, IOException {
        GroupedMessageFilter filter = GroupedMessageFilter.builder()
                .groupName(GROUP3_NAME)
                .bookId(bookId)
                .timestampFrom().isGreaterThanOrEqualTo(dataStart.plus(24, ChronoUnit.MINUTES))
                .timestampTo().isLessThan(dataStart.plus(28, ChronoUnit.MINUTES))
                .build();
        var actual = storage.getGroupedMessageBatches(filter);
        var resultAsList = Lists.newArrayList(actual.asIterable());
        Assertions.assertThat(resultAsList.get(0).getMessages().size()).isEqualTo(4);
    }

    @Test(description = "Get grouped messages withing interval with start time less than batch start time and end time less then batch end time")
    public void getGroupedMessagesWithHalfWideIntervalTest1() throws CradleStorageException, IOException {
        GroupedMessageFilter filter = GroupedMessageFilter.builder()
                .groupName(GROUP3_NAME)
                .bookId(bookId)
                .timestampFrom().isGreaterThanOrEqualTo(dataStart.plus(24, ChronoUnit.MINUTES))
                .timestampTo().isLessThan(dataStart.plus(29, ChronoUnit.MINUTES))
                .build();
        var actual = storage.getGroupedMessageBatches(filter);
        var resultAsList = Lists.newArrayList(actual.asIterable());
        Assertions.assertThat(resultAsList.get(0).getMessages().size()).isEqualTo(4);
    }

    @Test(description = "Get grouped messages withing interval with start time more than batch start time and end time more then batch end time")
    public void getGroupedMessagesWithHalfWideIntervalTest2() throws CradleStorageException, IOException {
        GroupedMessageFilter filter = GroupedMessageFilter.builder()
                .groupName(GROUP3_NAME)
                .bookId(bookId)
                .timestampFrom().isGreaterThanOrEqualTo(dataStart.plus(26, ChronoUnit.MINUTES))
                .timestampTo().isLessThan(dataStart.plus(30, ChronoUnit.MINUTES))
                .build();
        var actual = storage.getGroupedMessageBatches(filter);
        var resultAsList = Lists.newArrayList(actual.asIterable());
        Assertions.assertThat(resultAsList.get(0).getMessages().size()).isEqualTo(4);
    }

    @Test(description = "Get grouped messages withing interval with start time more than batch start time and end time less then batch end time")
    public void getGroupedMessagesWithNarrowIntervalTest3() throws CradleStorageException, IOException {
        GroupedMessageFilter filter = GroupedMessageFilter.builder()
                .groupName(GROUP3_NAME)
                .bookId(bookId)
                .timestampFrom().isGreaterThanOrEqualTo(dataStart.plus(26, ChronoUnit.MINUTES))
                .timestampTo().isLessThan(dataStart.plus(28, ChronoUnit.MINUTES))
                .build();
        var actual = storage.getGroupedMessageBatches(filter);
        var resultAsList = Lists.newArrayList(actual.asIterable());
        Assertions.assertThat(resultAsList.get(0).getMessages().size()).isEqualTo(4);
    }

    @Test(description = "Get grouped messages withing interval with start time more than batch start time and end time less then batch end time")
    public void getGroupedMessagesWithNarrowIntervalTest() throws CradleStorageException, IOException {
        GroupedMessageFilter filter = GroupedMessageFilter.builder()
                .groupName(GROUP3_NAME)
                .bookId(bookId)
                .timestampFrom().isGreaterThanOrEqualTo(dataStart.plus(26, ChronoUnit.MINUTES))
                .timestampTo().isLessThan(dataStart.plus(29, ChronoUnit.MINUTES))
                .build();
        var actual = storage.getGroupedMessageBatches(filter);
        var resultAsList = Lists.newArrayList(actual.asIterable());
        Assertions.assertThat(resultAsList.get(0).getMessages().size()).isEqualTo(4);
    }
}

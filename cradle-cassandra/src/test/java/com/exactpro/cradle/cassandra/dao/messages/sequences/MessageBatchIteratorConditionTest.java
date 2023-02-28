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

package com.exactpro.cradle.cassandra.dao.messages.sequences;

import com.exactpro.cradle.BookId;
import com.exactpro.cradle.Direction;
import com.exactpro.cradle.Order;
import com.exactpro.cradle.messages.MessageFilter;
import com.exactpro.cradle.messages.MessageFilterBuilder;
import com.exactpro.cradle.utils.CradleStorageException;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Set;

public class MessageBatchIteratorConditionTest {

    private static final SequenceRange SEQUENCE_RANGE = new SequenceRange(4, 17);
    private static final SequenceRangeExtractor<SequenceRange> EXTRACTOR = data -> data;

    private static final String BOOK_ID = "test_book";
    private static final String SESSION_ALIAS = "test_session";
    private static final Direction DIRECTION = Direction.FIRST;

    @DataProvider(name = "sequences")
    public Object[] sequences() {
        return new Object[]{3L, 4L, 10L, 17L, 20L};
    }


    @Test(dataProvider = "sequences")
    public void testEqualsDirect(Long sequence) throws CradleStorageException {
        MessageFilter messageFilter = beginMessageFilter().sequence().isEqualTo(sequence).order(Order.DIRECT).build();
        MessageBatchIteratorCondition<SequenceRange> iteratorCondition = new MessageBatchIteratorCondition<>(messageFilter, EXTRACTOR);
        check(sequence, iteratorCondition.test(SEQUENCE_RANGE), Set.of(4L, 10L, 17L, 20L));
    }

    @Test(dataProvider = "sequences")
    public void testEqualsReverse(Long sequence) throws CradleStorageException {
        MessageFilter messageFilter = beginMessageFilter().sequence().isEqualTo(sequence).order(Order.REVERSE).build();
        MessageBatchIteratorCondition<SequenceRange> iteratorCondition = new MessageBatchIteratorCondition<>(messageFilter, EXTRACTOR);
        check(sequence, iteratorCondition.test(SEQUENCE_RANGE), Set.of(3L, 4L, 10L, 17L));
    }

    @Test(dataProvider = "sequences")
    public void testGreaterDirect(Long sequence) throws CradleStorageException {
        MessageFilter messageFilter = beginMessageFilter().sequence().isGreaterThan(sequence).order(Order.DIRECT).build();
        MessageBatchIteratorCondition<SequenceRange> iteratorCondition = new MessageBatchIteratorCondition<>(messageFilter, EXTRACTOR);
        check(sequence, iteratorCondition.test(SEQUENCE_RANGE), Set.of(3L, 4L, 10L, 17L, 20L));
    }

    @Test(dataProvider = "sequences")
    public void testGreaterReverse(Long sequence) throws CradleStorageException {
        MessageFilter messageFilter = beginMessageFilter().sequence().isGreaterThan(sequence).order(Order.REVERSE).build();
        MessageBatchIteratorCondition<SequenceRange> iteratorCondition = new MessageBatchIteratorCondition<>(messageFilter, EXTRACTOR);
        check(sequence, iteratorCondition.test(SEQUENCE_RANGE), Set.of(3L, 4L, 10L));
    }

    @Test(dataProvider = "sequences")
    public void testGreaterOrEqualsDirect(Long sequence) throws CradleStorageException {
        MessageFilter messageFilter = beginMessageFilter().sequence().isGreaterThanOrEqualTo(sequence).order(Order.DIRECT).build();
        MessageBatchIteratorCondition<SequenceRange> iteratorCondition = new MessageBatchIteratorCondition<>(messageFilter, EXTRACTOR);
        check(sequence, iteratorCondition.test(SEQUENCE_RANGE), Set.of(3L, 4L, 10L, 17L, 20L));
    }

    @Test(dataProvider = "sequences")
    public void testGreaterOrEqualsReverse(Long sequence) throws CradleStorageException {
        MessageFilter messageFilter = beginMessageFilter().sequence().isGreaterThanOrEqualTo(sequence).order(Order.REVERSE).build();
        MessageBatchIteratorCondition<SequenceRange> iteratorCondition = new MessageBatchIteratorCondition<>(messageFilter, EXTRACTOR);
        check(sequence, iteratorCondition.test(SEQUENCE_RANGE), Set.of(3L, 4L, 10L, 17L));
    }

    @Test(dataProvider = "sequences")
    public void testLessDirect(Long sequence) throws CradleStorageException {
        MessageFilter messageFilter = beginMessageFilter().sequence().isLessThan(sequence).order(Order.DIRECT).build();
        MessageBatchIteratorCondition<SequenceRange> iteratorCondition = new MessageBatchIteratorCondition<>(messageFilter, EXTRACTOR);
        check(sequence, iteratorCondition.test(SEQUENCE_RANGE), Set.of(10L, 17L, 20L));
    }

    @Test(dataProvider = "sequences")
    public void testLessReverse(Long sequence) throws CradleStorageException {
        MessageFilter messageFilter = beginMessageFilter().sequence().isLessThan(sequence).order(Order.REVERSE).build();
        MessageBatchIteratorCondition<SequenceRange> iteratorCondition = new MessageBatchIteratorCondition<>(messageFilter, EXTRACTOR);
        check(sequence, iteratorCondition.test(SEQUENCE_RANGE), Set.of(3L, 4L, 10L, 17L, 20L));
    }

    @Test(dataProvider = "sequences")
    public void testLessOrEqualsDirect(Long sequence) throws CradleStorageException {
        MessageFilter messageFilter = beginMessageFilter().sequence().isLessThanOrEqualTo(sequence).order(Order.DIRECT).build();
        MessageBatchIteratorCondition<SequenceRange> iteratorCondition = new MessageBatchIteratorCondition<>(messageFilter, EXTRACTOR);
        check(sequence, iteratorCondition.test(SEQUENCE_RANGE), Set.of(4L, 10L, 17L, 20L));
    }

    @Test(dataProvider = "sequences")
    public void testLessOrEqualsReverse(Long sequence) throws CradleStorageException {
        MessageFilter messageFilter = beginMessageFilter().sequence().isLessThanOrEqualTo(sequence).order(Order.REVERSE).build();
        MessageBatchIteratorCondition<SequenceRange> iteratorCondition = new MessageBatchIteratorCondition<>(messageFilter, EXTRACTOR);
        check(sequence, iteratorCondition.test(SEQUENCE_RANGE), Set.of(3L, 4L, 10L, 17L, 20L));
    }

    private void check(long sequence, boolean result, Set<Long> validSequences) {
        Assert.assertEquals(result, validSequences.contains(sequence),
                String.format("MessageBatchIteratorCondition test for value %d should be [%b] but evaluates to [%b].",
                        sequence,
                        !result,
                        result));
    }


    private MessageFilterBuilder beginMessageFilter() {
        return new MessageFilterBuilder().bookId(new BookId(BOOK_ID))
                .sessionAlias(SESSION_ALIAS)
                .direction(DIRECTION);

    }
}
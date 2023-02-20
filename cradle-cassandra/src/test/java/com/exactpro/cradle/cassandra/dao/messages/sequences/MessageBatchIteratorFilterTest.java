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
import com.exactpro.cradle.messages.MessageFilter;
import com.exactpro.cradle.messages.MessageFilterBuilder;
import com.exactpro.cradle.utils.CradleStorageException;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Set;

public class MessageBatchIteratorFilterTest {

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
    public void testEquals(Long sequence) throws CradleStorageException {
        MessageFilter messageFilter = beginMessageFilter().sequence().isEqualTo(sequence).build();
        MessageBatchIteratorFilter<SequenceRange> iteratorFilter = new MessageBatchIteratorFilter<>(messageFilter, EXTRACTOR);
        check(sequence, iteratorFilter.test(SEQUENCE_RANGE), Set.of(4L, 10L, 17L));
    }

    @Test(dataProvider = "sequences")
    public void testGreater(Long sequence) throws CradleStorageException {
        MessageFilter messageFilter = beginMessageFilter().sequence().isGreaterThan(sequence).build();
        MessageBatchIteratorFilter<SequenceRange> iteratorFilter = new MessageBatchIteratorFilter<>(messageFilter, EXTRACTOR);
        check(sequence, iteratorFilter.test(SEQUENCE_RANGE), Set.of(3L, 4L, 10L));
    }

    @Test(dataProvider = "sequences")
    public void testGreaterOrEquals(Long sequence) throws CradleStorageException {
        MessageFilter messageFilter = beginMessageFilter().sequence().isGreaterThanOrEqualTo(sequence).build();
        MessageBatchIteratorFilter<SequenceRange> iteratorFilter = new MessageBatchIteratorFilter<>(messageFilter, EXTRACTOR);
        check(sequence, iteratorFilter.test(SEQUENCE_RANGE), Set.of(3L, 4L, 10L, 17L));
    }

    @Test(dataProvider = "sequences")
    public void testLess(Long sequence) throws CradleStorageException {
        MessageFilter messageFilter = beginMessageFilter().sequence().isLessThan(sequence).build();
        MessageBatchIteratorFilter<SequenceRange> iteratorFilter = new MessageBatchIteratorFilter<>(messageFilter, EXTRACTOR);
        check(sequence, iteratorFilter.test(SEQUENCE_RANGE), Set.of(10L, 17L, 20L));
    }

    @Test(dataProvider = "sequences")
    public void testLessOrEquals(Long sequence) throws CradleStorageException {
        MessageFilter messageFilter = beginMessageFilter().sequence().isLessThanOrEqualTo(sequence).build();
        MessageBatchIteratorFilter<SequenceRange> iteratorFilter = new MessageBatchIteratorFilter<>(messageFilter, EXTRACTOR);
        check(sequence, iteratorFilter.test(SEQUENCE_RANGE), Set.of(4L, 10L, 17L, 20L));
    }

    private void check(long sequence, boolean result, Set<Long> validSequences) {
        Assert.assertEquals(result, validSequences.contains(sequence),
                String.format("MessageBatchIteratorFilter test for value %d evaluates to %b but should be %b.",
                        sequence,
                        result,
                        !result));
    }

    private MessageFilterBuilder beginMessageFilter() {
        return new MessageFilterBuilder().bookId(new BookId(BOOK_ID))
                                            .sessionAlias(SESSION_ALIAS)
                                            .direction(DIRECTION);

    }
}
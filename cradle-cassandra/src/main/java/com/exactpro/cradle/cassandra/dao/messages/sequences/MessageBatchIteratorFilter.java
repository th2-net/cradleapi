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

import com.exactpro.cradle.filters.FilterForAny;
import com.exactpro.cradle.messages.MessageFilter;

import java.util.function.Predicate;

public class MessageBatchIteratorFilter<E> implements Predicate<E> {
    private final FilterForAny<Long> filter;
    private final SequenceRangeExtractor<E> extractor;

    public MessageBatchIteratorFilter(MessageFilter filter, SequenceRangeExtractor<E> extractor) {
        this.filter = filter == null ? null : filter.getSequence();
        this.extractor = extractor;
    }

    public static<E> MessageBatchIteratorFilter<E> none() {
        return new MessageBatchIteratorFilter<>(null, null);
    }

    @Override
    public boolean test(E value) {
        if (filter == null)
            return true;
        SequenceRange range = extractor.extract(value);
        long first = range.first;
        long last = range.last;
        long sequence = filter.getValue();
        switch (filter.getOperation()) {
            case EQUALS:
                return first <= sequence && sequence <= last;
            case GREATER:
                return last > sequence;
            case GREATER_OR_EQUALS:
                return last >= sequence;
            case LESS:
                return first < sequence;
            case LESS_OR_EQUALS:
                return first <= sequence;
        }
        return false;
    }
}
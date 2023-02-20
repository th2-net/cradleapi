/*
 * Copyright 2020-2023 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.cradle.iterators;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Predicate;

/**
 * Iterator which returns elements
 * from underlying iterator while predicate is true
 * @param <T>
 */
public class TakeWhileIterator<T> implements Iterator<T> {

    private final Predicate<? super T> predicate;
    private final Iterator<T> backingIterator;
    private T prefetched;
    private boolean halted;

    public TakeWhileIterator (Iterator<T> backingIterator, Predicate<? super T> predicate) {
        this.backingIterator = backingIterator;
        this.predicate = predicate;
        this.halted = false;
    }

    @Override
    public boolean hasNext() {
        if (halted) {
            return false;
        }

        if (prefetched != null) {
            return true;
        }

        if (backingIterator.hasNext()) {
            T next = backingIterator.next();

            if (predicate.test(next)) {
                prefetched = next;
                return true;
            }

            halted = true;
        }

        return false;
    }

    @Override
    public T next() {
        if (hasNext()) {
            T rtn = prefetched;
            prefetched = null;

            return rtn;
        }

        throw new NoSuchElementException("No more elements in iterator");
    }

    public boolean isHalted() {
        return halted;
    }
}

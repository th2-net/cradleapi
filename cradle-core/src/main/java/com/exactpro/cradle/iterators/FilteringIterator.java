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

import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;

import java.util.Iterator;

/**
 *  Iterator which filters elements from
 *  underlying iterator, according to predicate
 * @param <T>
 */
public class FilteringIterator<T> implements Iterator<T> {

    private final Iterator<T> wrapped;

    public FilteringIterator (Iterator<T> iterator, Predicate<? super T> retainIfTrue) {
        this.wrapped = Iterators.filter(iterator, retainIfTrue);
    }

    @Override
    public boolean hasNext() {
        return wrapped.hasNext();
    }

    @Override
    public T next() {
        return wrapped.next();
    }
}

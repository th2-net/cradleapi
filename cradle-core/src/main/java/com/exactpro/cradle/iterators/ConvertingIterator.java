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

import com.google.common.base.Function;
import com.google.common.collect.Iterators;

import java.util.Iterator;

/**
 * Iterator which transforms elements
 * from underlying iterator from one class to another
 * @param <F> type to convert from
 * @param <T> type to convert to
 */
public class ConvertingIterator<F, T> implements Iterator<T> {

    private final Iterator<T> wrapped;

    public ConvertingIterator(Iterator<F> iterator, Function<? super F, ? extends T> function) {
        this.wrapped = Iterators.transform(iterator, function);
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

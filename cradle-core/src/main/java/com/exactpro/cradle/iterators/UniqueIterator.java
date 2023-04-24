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

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * Iterator which returns skips duplicates
 * from underlying iterator
 * @param <T>
 */
public class UniqueIterator<T> extends FilteringIterator<T> {

    private static class UniquePredicate<T> implements Predicate<T> {
        private final Set<Integer> set;

        private UniquePredicate() {
            this(new HashSet<>());
        }

        private UniquePredicate(Set<Integer> set) {
            this.set = set;
        }

        @Override
        public boolean apply(T input) {
            if (input == null) {
                return false;
            }
            return set.add(input.hashCode());
        }
    }

    public UniqueIterator(Iterator<T> iterator) {
        super(iterator, new UniquePredicate<>());
    }

    public UniqueIterator(Iterator<T> iterator, Set<Integer> registry) {
        super(iterator, new UniquePredicate<>(registry));
    }
}
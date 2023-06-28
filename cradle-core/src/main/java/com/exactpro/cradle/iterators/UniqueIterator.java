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
 * ------------- USE WITH CAUTION ------------------------------------------------------
 * Since this iterator caches the values, it must be used only for primitives or strings
 * caching large set of full scaled database objects may cause OOM
 * @param <T>
 */
public class UniqueIterator<T> extends FilteringIterator<T> {

    private static class UniquePredicate<T> implements Predicate<T> {
        private final Set<T> set;

        private UniquePredicate() {
            this(new HashSet<>());
        }

        private UniquePredicate(Set<T> set) {
            this.set = set;
        }

        @Override
        public boolean apply(T input) {
            if (input == null) {
                return false;
            }
            return set.add(input);
        }
    }

    public UniqueIterator(Iterator<T> iterator) {
        super(iterator, new UniquePredicate<>());
    }

    public UniqueIterator(Iterator<T> iterator, Set<T> registry) {
        super(iterator, new UniquePredicate<>(registry));
    }
}

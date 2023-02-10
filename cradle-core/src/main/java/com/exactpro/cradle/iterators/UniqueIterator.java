package com.exactpro.cradle.iterators;


import com.google.common.base.Predicate;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class UniqueIterator<T> extends FilteringIterator<T> {

    private static class UniquePredicate<T> implements Predicate<T> {
        private final Set<Integer> set;

        private UniquePredicate() {
            this.set = new HashSet<>();
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
}

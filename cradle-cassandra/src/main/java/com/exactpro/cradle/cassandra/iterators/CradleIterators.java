package com.exactpro.cradle.cassandra.iterators;

import com.datastax.oss.driver.shaded.guava.common.base.Function;
import com.datastax.oss.driver.shaded.guava.common.base.Predicate;
import com.datastax.oss.driver.shaded.guava.common.collect.Iterators;

import java.util.HashSet;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

public class CradleIterators {

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

    private static class UnlessIterator<T> implements Iterator<T> {
        private T prefetched;
        private final Iterator<T> underlying;
        private final Predicate<? super T> unlessPredicate;

        private UnlessIterator(Iterator<T> underlying, Predicate<? super T> unlessPredicate) {
            this.underlying = underlying;
            this.unlessPredicate = unlessPredicate;
        }

        @Override
        public boolean hasNext() {
            if (prefetched != null && !unlessPredicate.test(prefetched)) {
                return true;
            }

            if (underlying.hasNext()) {
                prefetched = underlying.next();

                return !unlessPredicate.test(prefetched);
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
    }

    public static <T> Iterator<T> limit(Iterator<T> iterator, int limitSize) {
        return Iterators.limit(iterator, limitSize);
    }

    public static <T> Iterator<T> filter(Iterator<T> unfiltered, Predicate<? super T> retainIfTrue) {
        return Iterators.filter(unfiltered, retainIfTrue);
    }

    public static <F, T> Iterator<T> transform(Iterator<F> fromIterator, Function<? super F, ? extends T> function){
        return Iterators.transform(fromIterator, function);
    }

    public static <T> Iterator <T> unique(Iterator<T> iterator) {
        return filter(iterator, new UniquePredicate<>());
    }

    public static <T> Iterator<T> unless (Iterator<T> iterator, Predicate<? super T> unlessPredicate) {
        return new UnlessIterator<>(iterator, unlessPredicate);
    }
}

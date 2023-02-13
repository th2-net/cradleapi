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

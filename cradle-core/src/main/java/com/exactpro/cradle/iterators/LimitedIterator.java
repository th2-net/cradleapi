package com.exactpro.cradle.iterators;

import com.google.common.collect.Iterators;

import java.util.Iterator;

/**
 *  Iterator which limits passed iterator by limitSize,
 *  negative limit should not be passed
 * @param <T>
 */
public class LimitedIterator<T> implements Iterator<T> {

    private final Iterator<T> wrapped;

    public LimitedIterator (Iterator<T> iterator, int limitSize) {
        this.wrapped = Iterators.limit(iterator, limitSize);
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

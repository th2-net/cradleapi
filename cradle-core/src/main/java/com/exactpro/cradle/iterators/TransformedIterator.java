package com.exactpro.cradle.iterators;


import com.google.common.base.Function;
import com.google.common.collect.Iterators;

import java.util.Iterator;

public class TransformedIterator<F, T> implements Iterator<T> {

    private final Iterator<T> wrapped;

    public TransformedIterator (Iterator<F> iterator, Function<? super F, ? extends T> function) {
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

package com.exactpro.cradle.cassandra.iterators;

import com.datastax.oss.driver.api.core.MappedAsyncPagingIterable;
import com.datastax.oss.driver.api.core.cql.Row;
import com.exactpro.cradle.cassandra.retries.SelectQueryExecutor;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

//FIXME this iterator can be a potential bug source, having a Set<Long> as registry and calculating elements using
// passed hash function can lead to collisions and thus skipping non-duplicate element.
// we should deprecate this class and replace it's usage with DuplicateSkippingConvertingPagedIterator
public class DuplicateSkippingIterator<R, E> extends ConvertingPagedIterator<R, E>{

    private Set<Long> registry;
    private R preFetchedElement;
    private Function<R, Long> hashFunction;

    public DuplicateSkippingIterator(MappedAsyncPagingIterable<E> rows,
                                     SelectQueryExecutor selectExecutor,
                                     int limit,
                                     AtomicInteger returned,
                                     Function<E, R> converter,
                                     Function<Row, E> mapper,
                                     Function<R, Long> hashFunction,
                                     Set<Long> registry,
                                     String queryInfo) {
        super(rows, selectExecutor, limit, returned, converter, mapper, queryInfo);
        this.hashFunction = hashFunction;
        this.registry = registry == null ? new HashSet<>() : registry;
        this.preFetchedElement = null;
    }

    @Override
    public boolean hasNext() {
        if (preFetchedElement != null) {
            return true;
        }

        while (super.hasNext()) {
            R el = super.next();

            if (!registry.contains(hashFunction.apply(el))) {
                registry.add(hashFunction.apply(el));
                preFetchedElement = el;
                return true;
            }
        }

        return false;
    }

    @Override
    public R next() {
        if (!hasNext()) {
            return null;
        }

        R rtn = preFetchedElement;
        preFetchedElement = null;

        return rtn;
    }
}

package com.exactpro.cradle.cassandra.iterators;

import com.datastax.oss.driver.api.core.MappedAsyncPagingIterable;
import com.datastax.oss.driver.api.core.cql.Row;
import com.exactpro.cradle.cassandra.retries.SelectQueryExecutor;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

public class DuplicateSkippingIterator<R, E> extends ConvertingPagedIterator<R, E>{

    private Set<Long> set;
    private R preFetchedElement;
    private Function<R, Long> hashFunction;

    public DuplicateSkippingIterator(MappedAsyncPagingIterable<E> rows,
                                     SelectQueryExecutor selectExecutor,
                                     int limit,
                                     AtomicInteger returned,
                                     Function<E, R> converter,
                                     Function<Row, E> mapper,
                                     Function<R, Long> hashFunction,
                                     Set<Long> set,
                                     String queryInfo) {
        super(rows, selectExecutor, limit, returned, converter, mapper, queryInfo);
        this.hashFunction = hashFunction;
        this.set = set == null ? new HashSet<>() : set;
        this.preFetchedElement = null;
    }

    @Override
    public boolean hasNext() {
        if (preFetchedElement != null) {
            return true;
        }

        while (super.hasNext()) {
            R el = super.next();

            if (!set.contains(hashFunction.apply(el))) {
                set.add(hashFunction.apply(el));
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

    public Set<Long> getSet() {
        return set;
    }
}

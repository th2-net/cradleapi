package com.exactpro.cradle.cassandra.dao;

import com.exactpro.cradle.*;
import com.exactpro.cradle.cassandra.resultset.IteratorProvider;

import java.util.Iterator;
import java.util.concurrent.CompletableFuture;

public class CounterSampleIteratorProvider extends IteratorProvider<Counter> {

    public CounterSampleIteratorProvider(String requestInfo) {
        super(requestInfo);
    }

    @Override
    public CompletableFuture<Iterator<Counter>> nextIterator() {
        return CompletableFuture.completedFuture(null);
    }
}

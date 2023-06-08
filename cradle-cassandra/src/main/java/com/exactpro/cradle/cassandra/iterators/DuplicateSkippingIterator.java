/*
 * Copyright 2022-2023 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.cradle.cassandra.iterators;

import com.datastax.oss.driver.api.core.MappedAsyncPagingIterable;
import com.datastax.oss.driver.api.core.cql.Row;
import com.exactpro.cradle.cassandra.retries.SelectQueryExecutor;

import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

public class DuplicateSkippingIterator<R, E> extends ConvertingPagedIterator<R, E> {

    private final Set<R> registry;
    private R preFetchedElement;

    public DuplicateSkippingIterator(MappedAsyncPagingIterable<E> rows,
                                     SelectQueryExecutor selectExecutor,
                                     int limit,
                                     AtomicInteger returned,
                                     Function<E, R> converter,
                                     Function<Row, E> mapper,
                                     Set<R> registry,
                                     String queryInfo) {
        super(rows, selectExecutor, limit, returned, converter, mapper, queryInfo);
        this.registry = registry;
        this.preFetchedElement = null;
    }

    @Override
    public boolean hasNext() {
        if (preFetchedElement != null) {
            return true;
        }

        while (super.hasNext()) {
            R el = super.next();

            if (registry.add(el)) {
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

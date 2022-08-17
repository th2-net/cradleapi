/*
 * Copyright 2020-2022 Exactpro (Exactpro Systems Limited)
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/**
	Following iterator accepts additional skipFunction which is being
 	evaluated while getting next elements from underlying iterator
 	skipped elements do not get counted.
 */
public class SkippingConvertingPagedIterator<R, E> implements Iterator<R>
{
	private static final Logger logger = LoggerFactory.getLogger(SkippingConvertingPagedIterator.class);

	private final PagedIterator<E> it;
	private final int limit;
	private final AtomicInteger returned;
	private final Function<E, R> converter;
	private E preFetchedElement;
	private final Function<E, Boolean> skipFunction;

	public SkippingConvertingPagedIterator(MappedAsyncPagingIterable<E> rows,
										   SelectQueryExecutor selectExecutor,
										   int limit,
										   AtomicInteger returned,
										   Function<E, R> converter, Function<Row, E> mapper,
										   Function<E, Boolean> skipFunction,
										   String queryInfo)
	{
		this.it = new PagedIterator<>(rows, selectExecutor, mapper, queryInfo);
		this.limit = limit;
		this.returned = returned;
		this.converter = converter;
		this.skipFunction = skipFunction;
	}

	private boolean skipToValid() {
		if (!it.hasNext()) {
			return false;
		}

		E nextEl = it.next();

		while (it.hasNext() && skipFunction.apply(nextEl)) {
			logger.trace("Skipping element");
			nextEl = it.next();
		}

		// If this is true, it means that it.hasNext() is false, it has no more elements
		if (skipFunction.apply(nextEl)) {
			return false;
		}

		preFetchedElement = nextEl;

		return true;
	}

	@Override
	public R next() {
		if (limit > 0 && returned.get() >= limit) {
			throw new RuntimeException("`Limit` has been reached in iterator");
		}

		if (hasNext()) {
			E rtn = preFetchedElement;
			preFetchedElement = null;

			returned.incrementAndGet();

			return converter.apply(rtn);
		}

		throw new NoSuchElementException("There are no more elements in iterator");
	}


	@Override
	public boolean hasNext()
	{
		if (preFetchedElement != null) {
			return true;
		}

		if (!(limit <= 0 || returned.get() < limit) && it.hasNext()) {
			return false;
		}



		return skipToValid();
	}
}

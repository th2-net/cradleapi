/*
 * Copyright 2020-2023 Exactpro (Exactpro Systems Limited)
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
import java.util.function.Predicate;

/**
	Following iterator accepts additional skipFunction which is being
 	evaluated while getting next elements from underlying iterator
 	skipped elements do not get counted.
 */
public class FilteringConvertingPagedIterator<R, E> implements Iterator<R> {
	private static final Logger logger = LoggerFactory.getLogger(FilteringConvertingPagedIterator.class);

	private final PagedIterator<E> it;
	private final int limit;
	private final AtomicInteger returned;
	private final Function<E, R> converter;
	private R preFetchedElement;
	private final Predicate<R> filter;
	private final Predicate<R> iterationCondition;
	private boolean terminated;

	public FilteringConvertingPagedIterator(MappedAsyncPagingIterable<E> rows,
											SelectQueryExecutor selectExecutor,
											int limit,
											AtomicInteger returned,
											Function<E, R> converter, Function<Row, E> mapper,
											Predicate<R> filter,
											Predicate<R> iterationCondition,
											String queryInfo)
	{
		this.it = new PagedIterator<>(rows, selectExecutor, mapper, queryInfo);
		this.limit = limit;
		this.returned = returned;
		this.converter = converter;
		this.filter = filter;
		this.iterationCondition = iterationCondition;
	}


	private boolean checkIterationCondition(R value) {
		if (iterationCondition == null || iterationCondition.test(value)) {
			return true;
		}
		terminated = true;
		return false;
	}

	private boolean skipToValid() {
		if (terminated || !it.hasNext()) {
			return false;
		}

		R converted = converter.apply(it.next());
		if (!checkIterationCondition(converted)) {
			return false;
		}

		while (it.hasNext() && !filter.test(converted)) {
			logger.trace("Skipping element");
			converted = converter.apply(it.next());
			if (!checkIterationCondition(converted)) {
				return false;
			}
		}

		// If this is true, it means that it.hasNext() is false, it has no more elements
		if (!filter.test(converted)) {
			return false;
		}

		preFetchedElement = converted;
		return true;
	}

	@Override
	public R next() {
		if (limit > 0 && returned.get() >= limit) {
			throw new RuntimeException("`Limit` has been reached in iterator");
		}

		if (hasNext()) {
			R rtn = preFetchedElement;
			preFetchedElement = null;

			returned.incrementAndGet();
			return rtn;
		}

		throw new NoSuchElementException("There are no more elements in iterator");
	}


	@Override
	public boolean hasNext() {
		if (preFetchedElement != null) {
			return checkIterationCondition(preFetchedElement);
		}

		if (terminated) {
			return false;
		}

		if (!(limit <= 0 || returned.get() < limit) && it.hasNext()) {
			return false;
		}

		return skipToValid();
	}

	public boolean isTerminated() {
		return terminated;
	}
}

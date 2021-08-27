/*
 * Copyright 2021-2021 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.cradle.resultset;

import java.util.Iterator;

/**
 * Interface for objects that represent result of requests to Cradle to obtain some data
 */
public interface CradleResultSet<T> extends Iterator<T>
{
	/**
	 * Wraps this object into an {@link Iterable}. 
	 * Note that result set can only be iterated once, so each new call of this method doesn't reset iterator to the beginning of the result set.
	 * @return {@link Iterable} that wraps current state of result set iterator
	 */
	default Iterable<T> asIterable()
	{
		return () -> this;
	}
}

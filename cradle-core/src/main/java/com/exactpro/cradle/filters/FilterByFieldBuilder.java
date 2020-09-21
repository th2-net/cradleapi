/*
 * Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.cradle.filters;

/**
 * Builds filter value and returns object for next operations. Usable to build chains of filters.
 * For example, class {@code MultiFilterBuilder} contains set of filters, i.e. {@code Set<FilterByField<String>>}.
 * To define each filter, use {@code new FilterByFieldBuilder<String, MultiFilterBuilder>} where {@code String} is type of filter value.
 * {@code FilterByFieldBuilder} will return {@code MultiFilterBuilder}, allowing to define next filter.
 * @param <V> class of value to filter by
 * @param <R> class of object for next operations
 */
public abstract class FilterByFieldBuilder<V extends Comparable<V>, R>
{
	protected final R toReturn;
	protected FilterByField<V> filter;
	
	public FilterByFieldBuilder(FilterByField<V> filter, R toReturn)
	{
		this.filter = filter;
		this.toReturn = toReturn;
	}
	
	protected void setFilter(ComparisonOperation operation, V value)
	{
		filter.setOperation(operation);
		filter.setValue(value);
	}
}

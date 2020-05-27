/******************************************************************************
 * Copyright (c) 2009-2020, Exactpro Systems LLC
 * www.exactpro.com
 * Build Software to Test Software
 *
 * All rights reserved.
 * This is unpublished, licensed software, confidential and proprietary 
 * information which is the property of Exactpro Systems LLC or its licensors.
 ******************************************************************************/

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

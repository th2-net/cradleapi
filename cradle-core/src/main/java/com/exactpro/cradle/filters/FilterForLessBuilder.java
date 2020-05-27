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

public class FilterForLessBuilder<V extends Comparable<V>, R> extends FilterByFieldBuilder<V, R>
{
	public FilterForLessBuilder(FilterByField<V> filter, R toReturn)
	{
		super(filter, toReturn);
	}
	
	
	public R isLessThan(V value)
	{
		setFilter(ComparisonOperation.LESS, value);
		return toReturn;
	}
	
	public R isLessThanOrEqualTo(V value)
	{
		setFilter(ComparisonOperation.LESS_OR_EQUALS, value);
		return toReturn;
	}
}

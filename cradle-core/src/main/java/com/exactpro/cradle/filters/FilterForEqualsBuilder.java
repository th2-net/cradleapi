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

public class FilterForEqualsBuilder<V extends Comparable<V>, R> extends FilterByFieldBuilder<V, R>
{
	public FilterForEqualsBuilder(FilterForEquals<V> filter, R toReturn)
	{
		super(filter, toReturn);
	}
	
	public R isEqualTo(V value)
	{
		filter.setValue(value);
		return toReturn;
	}
}

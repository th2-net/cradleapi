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

public class FilterForGreater<V extends Comparable<V>> extends FilterByField<V>
{
	public FilterForGreater()
	{
	}
	
	public FilterForGreater(V value)
	{
		setValue(value);
	}
	
	
	public void setGreater()
	{
		setOperation(ComparisonOperation.GREATER);
	}
	
	public void setGreaterOrEquals()
	{
		setOperation(ComparisonOperation.GREATER_OR_EQUALS);
	}
}

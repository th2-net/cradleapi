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

public class FilterForAny<V extends Comparable<V>> extends FilterByField<V>
{
	public FilterForAny()
	{
	}
	
	public FilterForAny(V value, ComparisonOperation operation)
	{
		setValue(value);
		setOperation(operation);
	}
	
	
	@Override
	public void setOperation(ComparisonOperation operation)
	{
		super.setOperation(operation);
	}
}

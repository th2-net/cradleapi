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

public class FilterByField<V>
{
	private ComparisonOperation operation;
	private V value;
	
	
	public ComparisonOperation getOperation()
	{
		return operation;
	}
	
	public void setOperation(ComparisonOperation operation)
	{
		this.operation = operation;
	}
	
	
	public V getValue()
	{
		return value;
	}
	
	public void setValue(V value)
	{
		this.value = value;
	}
}

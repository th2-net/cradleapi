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

public class FilterByField<V extends Comparable<V>>
{
	private ComparisonOperation operation;
	private V value;
	
	public FilterByField()
	{
	}
	
	public FilterByField(V value, ComparisonOperation operation)
	{
		this.value = value;
		this.operation = operation;
	}
	
	
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
	
	
	public boolean check(V toCheck)
	{
		if (operation == null)
			return false;
		
		switch (operation)
		{
		case LESS : return toCheck.compareTo(value) < 0;
		case LESS_OR_EQUALS : return toCheck.compareTo(value) <= 0;
		case GREATER : return toCheck.compareTo(value) > 0;
		case GREATER_OR_EQUALS : return toCheck.compareTo(value) >= 0;
		case NOT_EQUALS : return !toCheck.equals(value);
		default : return toCheck.equals(value);
		}
	}
}

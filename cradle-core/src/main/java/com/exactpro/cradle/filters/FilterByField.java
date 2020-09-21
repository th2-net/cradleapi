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

public abstract class FilterByField<V extends Comparable<V>>
{
	private ComparisonOperation operation;
	private V value;
	
	
	public ComparisonOperation getOperation()
	{
		return operation;
	}
	
	protected void setOperation(ComparisonOperation operation)
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
		default : return toCheck.equals(value);
		}
	}
}

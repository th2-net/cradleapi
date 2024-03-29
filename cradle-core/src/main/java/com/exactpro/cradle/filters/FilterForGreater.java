/*
 * Copyright 2020-2021 Exactpro (Exactpro Systems Limited)
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

public class FilterForGreater<V extends Comparable<V>> extends FilterByField<V>
{
	public FilterForGreater()
	{
	}
	
	public FilterForGreater(V value)
	{
		setValue(value);
	}
	
	
	public static <V extends Comparable<V>> FilterForGreater<V> forGreater(V value)
	{
		FilterForGreater<V> result = new FilterForGreater<V>(value);
		result.setGreater();
		return result;
	}
	
	public static <V extends Comparable<V>> FilterForGreater<V> forGreaterOrEquals(V value)
	{
		FilterForGreater<V> result = new FilterForGreater<V>(value);
		result.setGreaterOrEquals();
		return result;
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

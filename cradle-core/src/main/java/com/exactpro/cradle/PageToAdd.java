/*
 * Copyright 2021-2021 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.cradle;

import java.time.Instant;

public class PageToAdd
{
	private final String name;
	private final Instant start;
	private final String comment;
	
	public PageToAdd(String name, Instant start, String comment)
	{
		this.name = name;
		this.start = start;
		this.comment = comment;
	}
	
	public String getName()
	{
		return name;
	}
	
	public Instant getStart()
	{
		return start;
	}
	
	public String getComment()
	{
		return comment;
	}

	@Override
	public String toString()
	{
		return "PageToAdd [name=" + name + ", start=" + start + ", comment=" + comment + "]";
	}
}

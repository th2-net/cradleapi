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

public class BookToAdd
{
	private final String name;
	private final Instant created;
	private final String firstPageName;
	private String fullName, 
			desc, 
			firstPageComment;
	
	public BookToAdd(String name, Instant created, String firstPageName)
	{
		this.name = name;
		this.created = created;
		this.firstPageName = firstPageName;
	}
	
	
	public String getName()
	{
		return name;
	}
	
	public Instant getCreated()
	{
		return created;
	}
	
	public String getFirstPageName()
	{
		return firstPageName;
	}
	
	
	public String getFullName()
	{
		return fullName;
	}
	
	public void setFullName(String fullName)
	{
		this.fullName = fullName;
	}
	
	
	public String getDesc()
	{
		return desc;
	}
	
	public void setDesc(String desc)
	{
		this.desc = desc;
	}
	
	
	public String getFirstPageComment()
	{
		return firstPageComment;
	}
	
	public void setFirstPageComment(String firstPageComment)
	{
		this.firstPageComment = firstPageComment;
	}
}
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

package com.exactpro.cradle.cassandra.resultset;

import java.util.Iterator;
import java.util.concurrent.CompletableFuture;

public abstract class IteratorProvider<T>
{
	private final String requestInfo;
	
	public IteratorProvider(String requestInfo)
	{
		this.requestInfo = requestInfo;
	}
	
	public abstract CompletableFuture<Iterator<T>> nextIterator();
	
	public String getRequestInfo()
	{
		return requestInfo;
	}
}

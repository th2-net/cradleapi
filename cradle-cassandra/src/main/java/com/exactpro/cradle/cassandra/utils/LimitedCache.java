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

package com.exactpro.cradle.cassandra.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.oss.driver.shaded.guava.common.cache.Cache;
import com.datastax.oss.driver.shaded.guava.common.cache.CacheBuilder;

/**
 * Cache with limited size that removes old elements when new element is added above limit
 */
public class LimitedCache<T>
{
	private static final Logger logger = LoggerFactory.getLogger(LimitedCache.class);
	
	private final Cache<T, T> cache;
	
	public LimitedCache(int limit)
	{
		cache = CacheBuilder.newBuilder().maximumSize(limit).build();
	}
	
	/**
	 * Adds given element to cache if it is not present in cache. If cache size exceeds its limit, old elements are removed from cache till cache size fits limit
	 * @param element to store in cache
	 * @return true if given element was absent in cache and has been added, false if element was present
	 */
	public boolean store(T element)
	{
		if (cache.getIfPresent(element) != null)
			return false;
		
		cache.put(element, element);
		if (logger.isTraceEnabled())
			logger.trace("Cache size after addition of '{}': {}", element, cache.size());
		return true;
	}
	
	/**
	 * Checks if given element is stored in cache
	 * @param element whose presence to check
	 * @return true if given element is present in cache, false otherwise
	 */
	public boolean contains(T element)
	{
		return cache.getIfPresent(element) != null;
	}
}

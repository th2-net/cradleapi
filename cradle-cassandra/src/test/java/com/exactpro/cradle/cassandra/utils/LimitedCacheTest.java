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

import org.testng.annotations.Test;
import org.testng.asserts.SoftAssert;

public class LimitedCacheTest
{
	@Test
	public void oldestIsRemoved()
	{
		LimitedCache<Integer> cache = new LimitedCache<>(3);
		
		cache.store(1);
		cache.store(2);
		cache.store(3);
		
		SoftAssert soft = new SoftAssert();
		soft.assertTrue(!cache.store(2), "cached element is not stored again");
		soft.assertTrue(cache.store(4), "new element is stored");
		
		soft.assertTrue(!cache.contains(1) || !cache.contains(2) || !cache.contains(3), "old element was removed");
		soft.assertTrue(cache.contains(4), "new element is still cached");
		soft.assertAll();
	}
}

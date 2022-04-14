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

package com.exactpro.cradle.testevents;

import org.assertj.core.api.Assertions;
import org.assertj.core.api.recursive.comparison.RecursiveComparisonConfiguration;

public class EventTestUtils
{
	public static void assertEvents(StoredTestEvent stored, TestEventToStore event)
	{
		RecursiveComparisonConfiguration config = new RecursiveComparisonConfiguration();
	  //These fields are present only in StoredTestEvent and will fail comparison with TestEventToStore
		config.ignoreFieldsMatchingRegexes("pageId", ".*\\.pageId", "error", ".*\\.error", "recDate", ".*\\.recDate");
		
		Assertions.assertThat(stored)
				.usingRecursiveComparison(config)
				.isEqualTo(event);
	}
}

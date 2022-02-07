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

package com.exactpro.cradle.cassandra;

import org.assertj.core.api.Assertions;
import org.testng.annotations.Test;

public class CassandraStorageSettingsTest
{
	@Test
	public void copy()
	{
		CassandraStorageSettings settings = new CassandraStorageSettings();
		settings.setBooksTable(settings.getBooksTable()+"_1");
		settings.setMaxUncompressedTestEventSize(settings.getMaxUncompressedTestEventSize()+10);
		
		CassandraStorageSettings copy = new CassandraStorageSettings(settings);
		Assertions.assertThat(copy)
				.usingRecursiveComparison()
				.isEqualTo(settings);
	}
}
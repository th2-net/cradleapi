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

package com.exactpro.cradle.cassandra.connection;

import org.assertj.core.api.Assertions;
import org.testng.annotations.Test;

public class CassandraConnectionSettingsTest
{
	@Test
	public void copy()
	{
		CassandraConnectionSettings settings = new CassandraConnectionSettings();
		settings.setHost("test");
		settings.setPort(9999);
		settings.setLocalDataCenter("center12");
		settings.setUsername("dummy_user");
		settings.setPassword("test_password_!@#$%");
		
		CassandraConnectionSettings copy = new CassandraConnectionSettings(settings);
		Assertions.assertThat(copy)
				.usingRecursiveComparison()
				.isEqualTo(settings);
	}
}

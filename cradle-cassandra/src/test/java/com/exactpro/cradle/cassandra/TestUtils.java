/*
 * Copyright 2021-2024 Exactpro (Exactpro Systems Limited)
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

import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.TraceEvent;
import com.exactpro.cradle.cassandra.integration.CassandraCradleHelper;
import com.exactpro.cradle.utils.CradleStorageException;

import java.util.Random;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TestUtils {
	private static final Random random = new Random();

	public static byte[] createContent(int size) {
		StringBuilder sb = new StringBuilder(size);
		for (int i = 0; i < size; i++)
			sb.append((char)(random.nextInt(10)+48));
		return sb.toString().getBytes();
	}

	private static final Pattern tombstonesPattern = Pattern.compile("Read (\\d+) live rows and (\\d+) tombstone cells");
	public static long countTombstones(CassandraCradleStorage storage, String table) throws CradleStorageException {
		ResultSet rs;
		try {
			rs = storage.getQueryExecutor().executeReadWithTracing("SELECT * FROM " + CassandraCradleHelper.KEYSPACE_NAME + '.' + table);
		} catch (IOException e) {
			throw new CradleStorageException("Failed to count tombstones", e);
		}

		long tombstonesSum = 0;
		for (TraceEvent event: rs.getExecutionInfo().getQueryTrace().getEvents()) {
			String activity = event.getActivity();
			if (activity == null) continue;

			Matcher matcher = tombstonesPattern.matcher(activity);
			if (matcher.matches()) {
				String tombstonesString = matcher.group(2);
				long tombstones = Long.parseLong(tombstonesString);
				tombstonesSum += tombstones;
			}
		}

		return tombstonesSum;
	}
}
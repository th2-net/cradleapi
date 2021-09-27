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

import com.exactpro.cradle.CradleStorage;
import com.exactpro.cradle.PageInfo;
import com.exactpro.cradle.utils.TimeUtils;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;


public class CassandraTimeUtils
{
	private static final DateTimeFormatter PART_FORMAT = DateTimeFormatter
			.ofPattern("yyyyMMddHHmm").withZone(CradleStorage.TIMEZONE_OFFSET);

	public static String getPart(LocalDateTime timestamp)
	{
		return timestamp.truncatedTo(ChronoUnit.HOURS).format(PART_FORMAT);
	}

	public static String getNextPart(String previousPart)
	{
		LocalDateTime parsed = LocalDateTime.parse(previousPart, PART_FORMAT);
		return getPart(parsed.plus(1, ChronoUnit.HOURS));
	}

	public static String getLastPart(PageInfo pageInfo)
	{
		LocalDateTime ldt = TimeUtils.toLocalTimestamp(pageInfo.getEnded() == null ? Instant.now() : pageInfo.getEnded());
		return getPart(ldt);
	}
}

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

package com.exactpro.cradle.utils;

import org.assertj.core.api.Assertions;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.time.Instant;
import java.time.LocalDate;
import java.util.List;

import static org.testng.Assert.assertEquals;

public class TimeUtilsTest
{

	@DataProvider(name = "splitByDate")
	protected Object[][] splitByDateData()
	{
		return new Object[][]{
				{Instant.parse("2021-01-03T10:15:30.00Z"), Instant.parse("2021-01-10T10:00:01.00Z"), 8, null},
				{Instant.parse("2021-01-03T10:15:30.00Z"), null, -1, null},
				{null, null, -1, CradleStorageException.class}
		};
	}
	
	@Test(dataProvider = "splitByDate")
	public void testSplitByDate(Instant from, Instant to, int expecteLength, Class<? extends Exception> eClass) throws CradleStorageException
	{
		if (eClass != null)
		{
			Assertions.assertThatExceptionOfType(eClass)
					.isThrownBy(() -> TimeUtils.splitByDate(from, to));
			return;
		}
		List<LocalDate> dates = TimeUtils.splitByDate(from, to);
		if (expecteLength > -1)
			assertEquals(dates.size(), expecteLength);
		LocalDate fromDate = null;
		boolean first = true;
		long day = 0L;
		for (LocalDate date : dates)
		{
			if (first)
			{
				first = false;
				fromDate = date;
				assertEquals(fromDate, TimeUtils.toLocalTimestamp(from).toLocalDate());
			}
			assertEquals(date, fromDate.plusDays(day++));
		}
	}
}
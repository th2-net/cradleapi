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

package com.exactpro.cradle.testevents;

import com.exactpro.cradle.BookId;
import com.exactpro.cradle.utils.CradleIdException;
import com.exactpro.cradle.utils.EscapeUtils;
import com.exactpro.cradle.utils.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Utilities to parse {@link StoredTestEventId} from its string representation which consists of timestamp:uniqueId
 */
public class StoredTestEventIdUtils
{
	private StoredTestEventIdUtils() { }
	public static List<String> splitParts(String id) throws CradleIdException
	{
		List<String> parts;
		try
		{
			parts = EscapeUtils.split(id);
		}
		catch (ParseException e)
		{
			throw new CradleIdException("Could not parse test event ID from string '"+id+"'", e);
		}
		
		if (parts.size() != 4)
			throw new CradleIdException("Test Event ID ("+id+") should contain book ID, scope, timestamp and unique ID "
					+ "delimited with '"+StoredTestEventId.ID_PARTS_DELIMITER+"'");
		return parts;
	}
	
	public static String getId(List<String> parts) throws CradleIdException
	{
		return parts.get(3);
	}
	
	public static Instant getTimestamp(List<String> parts) throws CradleIdException
	{
		String timeString = parts.get(2);
		try
		{
			return TimeUtils.fromIdTimestamp(timeString);
		}
		catch (DateTimeParseException e)
		{
			throw new CradleIdException("Invalid timstamp ("+timeString+") in ID '"+EscapeUtils.join(parts)+"'", e);
		}
	}
	
	public static String getScope(List<String> parts) throws CradleIdException
	{
		return parts.get(1);
	}
	
	public static BookId getBook(List<String> parts)
	{
		return new BookId(parts.get(0));
	}
	
	public static String timestampToString(Instant timestamp)
	{
		return TimeUtils.toIdTimestamp(timestamp);
	}

	public static String logId(TestEvent event) {
		return event.getBookId().getName() + ':' +
				event.getScope() + ':' +
				event.getStartTimestamp() + ':' +
				event.getId() + " - " + event.getName();
	}

	private static final Logger LOGGER = LoggerFactory.getLogger(StoredTestEventIdUtils.class);

	public static void track(String id, String text) {
//		LOGGER.error("Track: " + text + ' ' + id + ' ' + System.nanoTime());
	}
	public static void track(TestEventToStore event, String text) {
//		String id;
//		if (event.isBatch()) {
//			id = event.asBatch().getTestEvents().iterator().next().getId().getId();
//		} else {
//			id = event.id.getId();
//		}
//		track(id, text);
	}

	public static class Statistic {
		private static final Map<String, MeasureData> DATA = new ConcurrentHashMap<>();

		public static AutoCloseable measure(String measureName) {
			MeasureData measureData = DATA.computeIfAbsent(measureName, MeasureData::new);
			return measureData.measure();
		}
	}

	private static class MeasureData {
		public static final int NANOS_IN_SECOND = 1_000_000_000;
		private final Lock lock = new ReentrantLock();
		private final String name;
		private double count = 0;
		private long sum = 0;
		private long lastPrint = System.nanoTime();
        private MeasureData(String name) {
            this.name = name;
        }

		public AutoCloseable measure() {
			return new Measure();
		}

		public void update(long now, long duration) {
			lock.lock();
			try {
				count++;
				sum += duration;
				if (now - lastPrint > NANOS_IN_SECOND) {
					LOGGER.error("Track (" + name + ") count: " + count + ", sum: " + sum +
							", rate: " + (count/sum*NANOS_IN_SECOND + " times/sec"));
					count = 0;
					sum = 0;
					lastPrint = now;
				}
			} finally {
				lock.unlock();
			}
		}

		private class Measure implements AutoCloseable {
			private final long start = System.nanoTime();

			@Override
			public void close() {
				long now = System.nanoTime();
				update(now, now - start);
			}
		}
    }
}

/*
 * Copyright 2022-2024 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.cradle.FrameType;
import com.exactpro.cradle.cassandra.counters.FrameInterval;
import com.exactpro.cradle.counters.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;

import static com.exactpro.cradle.CradleStorage.TIMEZONE_OFFSET;
import static com.exactpro.cradle.cassandra.CassandraStorageSettings.MAX_EPOCH_INSTANT;
import static com.exactpro.cradle.cassandra.CassandraStorageSettings.MIN_EPOCH_INSTANT;

public class StorageUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(StorageUtils.class);

    private StorageUtils() { }

    public static List<FrameInterval> sliceInterval (Interval interval) {
        List<FrameInterval> slices = new ArrayList<>();

        FrameType[] frameTypes = FrameType.values();
        int minFrameIndex = 0;
        int maxFrameIndex = FrameType.values().length - 1;
        int frameIndex = maxFrameIndex;
        Instant start = frameTypes[minFrameIndex].getFrameStart(interval.getStart());
        Instant end = frameTypes[minFrameIndex].getFrameStart(
                interval.getEnd().plusMillis(frameTypes[minFrameIndex].getMillisInFrame()).minusMillis(1));


        FrameType frameType = frameTypes[frameIndex];
		/*
		  Adjust frame for frame start value
		  i.e. if start time is on second mark
		  we should start with FrameType.SECOND frames
		 */
        while (frameIndex > minFrameIndex && !frameType.getFrameStart(start).equals(start)) {
            frameIndex --;
            frameType = frameTypes[frameIndex];
        }
		/*
			Create requests for smaller frame types at the start
		 	Should try to increase granularity until
			we're at the biggest possible frames
		 */
        Instant fStart = start, fEnd;
        while (frameIndex < maxFrameIndex) {
            frameType = frameTypes[frameIndex];
            fStart = frameType.getFrameStart(fStart);
            fEnd = frameTypes[frameIndex + 1].getFrameEnd(fStart);

            if (fEnd.isAfter(end)) {
                break;
            }

            if (!fStart.equals(fEnd)) {
                slices.add(new FrameInterval(frameType, new Interval(fStart, fEnd)));
            }

            fStart = fEnd;
            frameIndex ++;
        }

        // Create request for biggest possible frame type
        frameType = frameTypes[frameIndex];
        fEnd = frameTypes[frameIndex].getFrameStart(end);
        if (!fStart.equals(fEnd)) {
            slices.add(new FrameInterval(frameType, new Interval(fStart, fEnd)));
        }

        // Create requests for smaller frame types at the end
        while (frameIndex > 0) {
			/*
				each step we should decrease granularity and fill
				interval with smaller frames
			 */
            frameIndex --;
            frameType = frameTypes[frameIndex];

            fStart = fEnd;
			/*
				Unless we are querying the smallest granularity,
				we should leave interval for smaller frames
			 */
            fEnd = frameType.getFrameStart(end);
			/*
				Current granularity exhausted interval,
				i.e. end was set at second etc.
			 */
            if (end.isBefore(fEnd)) {
                break;
            }

            if (!fStart.equals(fEnd)) {
                slices.add(new FrameInterval(frameType, new Interval(fStart, fEnd)));
            }
        }

        return slices;
    }

    public static LocalDate toLocalDate(Instant instant) {
        if (instant.isBefore(MIN_EPOCH_INSTANT)) {
            LOGGER.trace("Replaces {} by {}", instant, MIN_EPOCH_INSTANT);
            return LocalDate.MIN;
        }
        if (instant.isAfter(MAX_EPOCH_INSTANT)) {
            LOGGER.trace("Replaces {} by {}", instant, MAX_EPOCH_INSTANT);
            return LocalDate.MAX;
        }
        return LocalDate.ofInstant(instant, TIMEZONE_OFFSET);
    }

    public static LocalTime toLocalTime(Instant instant) {
        if (instant.isBefore(MIN_EPOCH_INSTANT)) {
            LOGGER.trace("Replaces {} by {}", instant, MIN_EPOCH_INSTANT);
            return LocalTime.MIN;
        }
        if (instant.isAfter(MAX_EPOCH_INSTANT)) {
            LOGGER.trace("Replaces {} by {}", instant, MAX_EPOCH_INSTANT);
            return LocalTime.MAX;
        }
        return LocalTime.ofInstant(instant, TIMEZONE_OFFSET);
    }

    public static LocalDateTime toLocalDateTime(Instant instant) {
        if (instant.isBefore(MIN_EPOCH_INSTANT)) {
            LOGGER.trace("Replaces {} by {}", instant, MIN_EPOCH_INSTANT);
            return LocalDateTime.MIN;
        }
        if (instant.isAfter(MAX_EPOCH_INSTANT)) {
            LOGGER.trace("Replaces {} by {}", instant, MAX_EPOCH_INSTANT);
            return LocalDateTime.MAX;
        }
        return LocalDateTime.ofInstant(instant, TIMEZONE_OFFSET);
    }
}

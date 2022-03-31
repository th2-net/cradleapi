/*
 * Copyright 2020-2022 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.cradle.FrameType;
import com.exactpro.cradle.cassandra.counters.FrameInterval;
import com.exactpro.cradle.counters.Interval;
import org.testng.annotations.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.testng.Assert.*;

public class CassandraCradleStorageTest {

    @Test
    private void testEmptyInterval () {
        Instant start, end;
        start = Instant.parse("2022-03-22T17:05:37.100Z");
        end  = Instant.parse("2022-03-22T17:05:37.100Z");

        List<FrameInterval> actual, expected;
        actual = CassandraCradleStorage.sliceInterval(new Interval(start, end));
        expected = new ArrayList<>();

        assertEquals(actual, expected);
    }

    @Test
    public void testOnlyMillisecondFramesInterval () {
        Instant start, end;
        start = Instant.parse("2022-03-22T17:05:37.100Z");
        end  = Instant.parse("2022-03-22T17:05:38.596Z");

        List<FrameInterval> actual, expected;
        actual = CassandraCradleStorage.sliceInterval(new Interval(start, end));
        expected = new ArrayList<>();
        expected.add(new FrameInterval(FrameType.TYPE_100MS,
                new Interval(Instant.parse("2022-03-22T17:05:37.100Z"),
                        Instant.parse("2022-03-22T17:05:38.000Z"))));
        expected.add(new FrameInterval(FrameType.TYPE_100MS,
                new Interval(Instant.parse("2022-03-22T17:05:38.000Z"),
                        Instant.parse("2022-03-22T17:05:38.600Z"))));

        assertEquals(actual, expected);
    }

    @Test
    public void testOnlySecondFramesInterval () {
        Instant start, end;
        start = Instant.parse("2022-03-22T17:05:25.000Z");
        end  = Instant.parse("2022-03-22T17:06:59.000Z");

        List<FrameInterval> actual, expected;
        actual = CassandraCradleStorage.sliceInterval(new Interval(start, end));
        expected = new ArrayList<>();
        expected.add(new FrameInterval(FrameType.TYPE_SECOND,
                new Interval(Instant.parse("2022-03-22T17:05:25.000Z"),
                        Instant.parse("2022-03-22T17:06:00.000Z"))));
        expected.add(new FrameInterval(FrameType.TYPE_SECOND,
                new Interval(Instant.parse("2022-03-22T17:06:00.000Z"),
                        Instant.parse("2022-03-22T17:06:59.000Z"))));

        assertEquals(actual, expected);
    }

    @Test
    public void testOnlyMinuteFramesInterval () {
        Instant start, end;
        start = Instant.parse("2022-03-22T17:05:00.000Z");
        end  = Instant.parse("2022-03-22T18:23:00.000Z");

        List<FrameInterval> actual, expected;
        actual = CassandraCradleStorage.sliceInterval(new Interval(start, end));
        expected = new ArrayList<>();
        expected.add(new FrameInterval(FrameType.TYPE_MINUTE,
                new Interval(Instant.parse("2022-03-22T17:05:00.000Z"),
                        Instant.parse("2022-03-22T18:00:00.000Z"))));
        expected.add(new FrameInterval(FrameType.TYPE_MINUTE,
                new Interval(Instant.parse("2022-03-22T18:00:00.000Z"),
                        Instant.parse("2022-03-22T18:23:00.000Z"))));

        assertEquals(actual, expected);
    }

    @Test
    public void testOnlyHoursFramesInterval () {
        Instant start, end;
        start = Instant.parse("2022-03-22T17:00:00.025Z");
        end  = Instant.parse("2022-03-24T20:00:00.420Z");

        List<FrameInterval> actual, expected;
        actual = CassandraCradleStorage.sliceInterval(new Interval(start, end));
        expected = new ArrayList<>();
        expected.add(new FrameInterval(FrameType.TYPE_HOUR,
                new Interval(Instant.parse("2022-03-22T17:00:00.000Z"),
                        Instant.parse("2022-03-24T20:00:00.000Z"))));

        assertEquals(actual, expected);
    }

    @Test
    public void testUptoSecondFramesInterval () {
        Instant start, end;
        start = Instant.parse("2022-03-22T17:05:37.128Z");
        end  = Instant.parse("2022-03-22T17:05:40.250Z");

        List<FrameInterval> actual, expected;
        actual = CassandraCradleStorage.sliceInterval(new Interval(start, end));
        expected = new ArrayList<>();
        expected.add(new FrameInterval(FrameType.TYPE_100MS,
                new Interval(Instant.parse("2022-03-22T17:05:37.100Z"),
                        Instant.parse("2022-03-22T17:05:38.000Z"))));
        expected.add(new FrameInterval(FrameType.TYPE_SECOND,
                new Interval(Instant.parse("2022-03-22T17:05:38.000Z"),
                        Instant.parse("2022-03-22T17:05:40.000Z"))));
        expected.add(new FrameInterval(FrameType.TYPE_100MS,
                new Interval(Instant.parse("2022-03-22T17:05:40.000Z"),
                        Instant.parse("2022-03-22T17:05:40.300Z"))));

        assertEquals(actual, expected);
    }

    @Test
    public void testUptoMinuteFramesInterval () {
        Instant start, end;
        start = Instant.parse("2022-03-22T17:05:37.128Z");
        end  = Instant.parse("2022-03-22T17:15:40.250Z");

        List<FrameInterval> actual, expected;
        actual = CassandraCradleStorage.sliceInterval(new Interval(start, end));
        expected = new ArrayList<>();
        expected.add(new FrameInterval(FrameType.TYPE_100MS,
                new Interval(Instant.parse("2022-03-22T17:05:37.100Z"),
                        Instant.parse("2022-03-22T17:05:38.000Z"))));
        expected.add(new FrameInterval(FrameType.TYPE_SECOND,
                new Interval(Instant.parse("2022-03-22T17:05:38.000Z"),
                        Instant.parse("2022-03-22T17:06:00.000Z"))));
        expected.add(new FrameInterval(FrameType.TYPE_MINUTE,
                new Interval(Instant.parse("2022-03-22T17:06:00.000Z"),
                        Instant.parse("2022-03-22T17:15:00.000Z"))));
        expected.add(new FrameInterval(FrameType.TYPE_SECOND,
                new Interval(Instant.parse("2022-03-22T17:15:00.000Z"),
                        Instant.parse("2022-03-22T17:15:40.000Z"))));
        expected.add(new FrameInterval(FrameType.TYPE_100MS,
                new Interval(Instant.parse("2022-03-22T17:15:40.000Z"),
                        Instant.parse("2022-03-22T17:15:40.300Z"))));

        assertEquals(actual, expected);
    }

    @Test
    public void testUptoHourFramesInterval () {

        final Instant start = Instant.parse("2022-03-22T17:05:37.128Z");
        final Instant end   = Instant.parse("2022-03-22T20:15:40.250Z");

        final List<FrameInterval> actual = CassandraCradleStorage.sliceInterval(new Interval(start, end));
        List<FrameInterval> expected;
        expected = new ArrayList<>();
        expected.add(new FrameInterval(FrameType.TYPE_100MS,
                new Interval(Instant.parse("2022-03-22T17:05:37.100Z"),
                        Instant.parse("2022-03-22T17:05:38.000Z"))));
        expected.add(new FrameInterval(FrameType.TYPE_SECOND,
                new Interval(Instant.parse("2022-03-22T17:05:38.000Z"),
                        Instant.parse("2022-03-22T17:06:00.000Z"))));
        expected.add(new FrameInterval(FrameType.TYPE_MINUTE,
                new Interval(Instant.parse("2022-03-22T17:06:00.000Z"),
                        Instant.parse("2022-03-22T18:00:00.000Z"))));
        expected.add(new FrameInterval(FrameType.TYPE_HOUR,
                new Interval(Instant.parse("2022-03-22T18:00:00.000Z"),
                        Instant.parse("2022-03-22T20:00:00.000Z"))));
        expected.add(new FrameInterval(FrameType.TYPE_MINUTE,
                new Interval(Instant.parse("2022-03-22T20:00:00.000Z"),
                        Instant.parse("2022-03-22T20:15:00.000Z"))));
        expected.add(new FrameInterval(FrameType.TYPE_SECOND,
                new Interval(Instant.parse("2022-03-22T20:15:00.000Z"),
                        Instant.parse("2022-03-22T20:15:40.000Z"))));
        expected.add(new FrameInterval(FrameType.TYPE_100MS,
                new Interval(Instant.parse("2022-03-22T20:15:40.000Z"),
                        Instant.parse("2022-03-22T20:15:40.300Z"))));

        validateSlices(expected, start, end, 7);
        assertEquals(actual, expected);
    }


    private void validateSlices(List<FrameInterval> actual,
                                Instant start,
                                Instant end,
                                int expectedSize) {

        final int size = actual.size();
        final FrameType minFrame = FrameType.values()[0];

        assertEquals(actual.get(0).getInterval().getStart(), minFrame.getFrameStart(start), "Wrong start time of the series:");
        assertEquals(actual.get(size - 1).getInterval().getEnd(), minFrame.getFrameEnd(end), "Wrong end time of the series:");
        assertEquals(size, expectedSize, "Wrong number of intervals:");
        validateContinuity(actual);
    }


    private void validateInterval(FrameInterval interval) {
        FrameType frameType = interval.getFrameType();
        Instant start = interval.getInterval().getStart();
        Instant end = interval.getInterval().getEnd();
        if (!start.isBefore(end))
            fail(String.format("Interval start must be strictly before its end: start=%s, end=%s", start, end));

        assertEquals(start, frameType.getFrameStart(start), String.format("Interval start does not align to FrameType.%s start:", frameType));
        assertEquals(start, frameType.getFrameStart(start), String.format("Interval end does not align to FrameType.%s end:", frameType));
    }


    private void validateContinuity(List<FrameInterval> intervals) {
        FrameInterval last = null;
        for (FrameInterval el : intervals) {
            validateInterval(el);
            if (last != null) {
                assertNotEquals(el.getFrameType(), last.getFrameType(), String.format("Identical frame types[%s] on adjacent elements", el.getInterval().getStart()));
                assertEquals(el.getInterval().getStart(), last.getInterval().getEnd(), "Sequence continuity failure:");
            }
            last = el;
        }
    }
}

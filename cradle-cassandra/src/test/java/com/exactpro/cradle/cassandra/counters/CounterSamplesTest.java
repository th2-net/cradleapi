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
package com.exactpro.cradle.cassandra.counters;

import com.exactpro.cradle.Counter;
import com.exactpro.cradle.FrameType;
import org.testng.annotations.Test;

import java.time.Instant;
import java.util.Arrays;
import java.util.Comparator;

import static org.testng.Assert.assertEquals;

public class CounterSamplesTest {
    @Test
    void testEmptyExtractAll() {
        CounterSamples samples = new CounterSamples(FrameType.TYPE_HOUR);
        assertEquals(samples.extractAll().size(), 0);
    }

    @Test
    void testDoubleExtractAll() {
        CounterSamples samples = new CounterSamples(FrameType.TYPE_MINUTE);
        Instant t1 = Instant.parse("2022-03-15T23:59:58.987Z");
        Instant t2 = Instant.parse("2022-03-15T23:59:12.000Z");
        Instant t3 = Instant.parse("2022-03-15T23:55:12.000Z");

        samples.update(t1, new Counter(100, 1000));
        samples.update(t2, new Counter(200, 2000));
        samples.update(t3, new Counter(500, 5000));

        assertEquals(samples.extractAll().size(), 2);
        assertEquals(samples.extractAll().size(), 0);
    }


    private TimeFrameCounter[] extractAllAsSortedArray(CounterSamples samples) {
        TimeFrameCounter[] data = samples.extractAll().toArray(new TimeFrameCounter[0]);
        Arrays.sort(data, Comparator.comparing(TimeFrameCounter::getFrameStart));
        return data;
    }


    @Test
    void testFrameType_100MS() {
        CounterSamples samples = new CounterSamples(FrameType.TYPE_100MS);
        Instant t1 = Instant.parse("2022-03-15T23:59:12.987Z");
        Instant t2 = Instant.parse("2022-03-15T23:59:12.900Z");
        Instant t3 = Instant.parse("2022-03-15T23:59:12.873Z");

        Counter c1 = new Counter(100, 1000);
        Counter c2 = new Counter(200, 2000);
        Counter c3 = new Counter(500, 5000);
        samples.update(t1, c1);
        samples.update(t2, c2);
        samples.update(t3, c3);

        TimeFrameCounter[] data = extractAllAsSortedArray(samples);
        assertEquals(data.length, 2);

        assertEquals(data[0].getFrameStart(), Instant.parse("2022-03-15T23:59:12.800Z"));
        assertEquals(data[0].getCounter(), c3);

        assertEquals(data[1].getFrameStart(), Instant.parse("2022-03-15T23:59:12.900Z"));
        assertEquals(data[1].getCounter(), new Counter(c1.getEntityCount() + c2.getEntityCount(), c1.getEntitySize() + c2.getEntitySize()));
    }


    @Test
    void testFrameType_SECOND() {
        CounterSamples samples = new CounterSamples(FrameType.TYPE_SECOND);
        Instant t1 = Instant.parse("2022-03-15T23:59:58.987Z");
        Instant t2 = Instant.parse("2022-03-15T23:59:12.230Z");
        Instant t3 = Instant.parse("2022-03-15T23:59:12.200Z");
        Instant t4 = Instant.parse("2022-03-15T13:59:12.330Z");

        Counter c1 = new Counter(100, 1000);
        Counter c2 = new Counter(200, 2000);
        Counter c3 = new Counter(500, 5000);
        Counter c4 = new Counter(600, 6000);
        samples.update(t1, c1);
        samples.update(t2, c2);
        samples.update(t3, c3);
        samples.update(t4, c4);

        TimeFrameCounter[] data = extractAllAsSortedArray(samples);
        assertEquals(data.length, 3);

        assertEquals(data[0].getFrameStart(), Instant.parse("2022-03-15T13:59:12.000Z"));
        assertEquals(data[0].getCounter(), c4);

        assertEquals(data[1].getFrameStart(), Instant.parse("2022-03-15T23:59:12.000Z"));
        assertEquals(data[1].getCounter(), new Counter(c3.getEntityCount() + c2.getEntityCount(), c3.getEntitySize() + c2.getEntitySize()));

        assertEquals(data[2].getFrameStart(), Instant.parse("2022-03-15T23:59:58.000Z"));
        assertEquals(data[2].getCounter(), c1);
    }

    @Test
    void testFrameType_MINUTE() {
        CounterSamples samples = new CounterSamples(FrameType.TYPE_MINUTE);
        Instant t1 = Instant.parse("2022-03-15T23:59:58.987Z");
        Instant t2 = Instant.parse("2022-03-15T23:59:12.000Z");
        Instant t3 = Instant.parse("2022-03-15T23:55:12.002Z");

        Counter c1 = new Counter(100, 1000);
        Counter c2 = new Counter(200, 2000);
        Counter c3 = new Counter(500, 5000);
        samples.update(t1, c1);
        samples.update(t2, c2);
        samples.update(t3, c3);

        TimeFrameCounter[] data = extractAllAsSortedArray(samples);
        assertEquals(data.length, 2);

        assertEquals(data[0].getFrameStart(), Instant.parse("2022-03-15T23:55:00.000Z"));
        assertEquals(data[0].getCounter(), c3);

        assertEquals(data[1].getFrameStart(), Instant.parse("2022-03-15T23:59:00.000Z"));
        assertEquals(data[1].getCounter(), new Counter(c1.getEntityCount() + c2.getEntityCount(), c1.getEntitySize() + c2.getEntitySize()));
    }

    @Test
    void testFrameType_HOUR() {
        CounterSamples samples = new CounterSamples(FrameType.TYPE_HOUR);
        Instant t1 = Instant.parse("2022-03-17T23:29:58.987Z");
        Instant t2 = Instant.parse("2022-03-15T23:29:12.000Z");
        Instant t3 = Instant.parse("2022-03-15T23:55:12.120Z");

        Counter c1 = new Counter(100, 1000);
        Counter c2 = new Counter(200, 2000);
        Counter c3 = new Counter(500, 5000);
        samples.update(t1, c1);
        samples.update(t2, c2);
        samples.update(t3, c3);

        TimeFrameCounter[] data = extractAllAsSortedArray(samples);
        assertEquals(data.length, 2);

        assertEquals(data[0].getFrameStart(), Instant.parse("2022-03-15T23:00:00.000Z"));
        assertEquals(data[0].getCounter(), new Counter(c3.getEntityCount() + c2.getEntityCount(), c3.getEntitySize() + c2.getEntitySize()));

        assertEquals(data[1].getFrameStart(), Instant.parse("2022-03-17T23:00:00.000Z"));
        assertEquals(data[1].getCounter(), c1);
    }
}

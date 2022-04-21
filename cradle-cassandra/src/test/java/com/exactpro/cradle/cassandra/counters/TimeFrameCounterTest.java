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

import com.exactpro.cradle.counters.Counter;
import org.testng.annotations.Test;

import java.time.Instant;

import static org.testng.Assert.assertEquals;

public class TimeFrameCounterTest {

    @Test
    void testFrameStart() {
        Instant t = Instant.parse("2022-03-15T23:59:58.987Z");
        CounterTimeFrameRecord tfc = new CounterTimeFrameRecord(t, new Counter(123, 345));
        assertEquals(tfc.getFrameStart(), t);
    }

    @Test
    void testCounter() {
        Counter counter = new Counter(123, 456);
        Instant t = Instant.parse("2022-03-15T23:59:58.987Z");
        CounterTimeFrameRecord tfc = new CounterTimeFrameRecord(t, counter);
        assertEquals(tfc.getRecord(), counter);
    }

    @Test
    void testIncrementBy() {
        long ec1 = 123, es1 = 3456;
        long ec2 = 4813491, es2 = 9027073;
        Instant t = Instant.parse("2022-03-15T23:59:58.987Z");

        CounterTimeFrameRecord tfc = new CounterTimeFrameRecord(t, new Counter(ec1, es1));
        tfc.update(new Counter(ec2, es2));
        Counter counter = new Counter(ec1 + ec2, es1 + es2);
        assertEquals(tfc.getRecord(), counter);
    }
}

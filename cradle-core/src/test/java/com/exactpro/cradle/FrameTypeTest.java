package com.exactpro.cradle;

import org.testng.annotations.Test;

import java.time.Instant;

import static org.testng.Assert.assertEquals;

public class FrameTypeTest {
    @Test
    public void testFrom() {
        assertEquals(FrameType.from(1), FrameType.TYPE_100MS);
        assertEquals(FrameType.from(2), FrameType.TYPE_SECOND);
        assertEquals(FrameType.from(3), FrameType.TYPE_MINUTE);
        assertEquals(FrameType.from(4), FrameType.TYPE_HOUR);
    }

    @Test
    public void testFrameStart100ms() {
        String[][] tests = {
                {"2022-03-14T12:23:23.345Z", "2022-03-14T12:23:23.300Z"},
                {"2022-03-14T00:00:59.982Z", "2022-03-14T00:00:59.900Z"},
                {"2022-03-14T12:00:00.999Z", "2022-03-14T12:00:00.900Z"},
                {"2022-03-14T12:00:00.000Z", "2022-03-14T12:00:00.000Z"},
                {"2022-03-14T12:00:00.101Z", "2022-03-14T12:00:00.100Z"},
                {"2022-03-14T12:00:00.099Z", "2022-03-14T12:00:00.000Z"},
                {"2022-03-14T00:00:59.000Z", "2022-03-14T00:00:59.000Z"},
        };
        for (int t = 0; t < tests.length; t++)
            assertEquals(FrameType.TYPE_100MS.getFrameStart(Instant.parse(tests[t][0])), Instant.parse(tests[t][1]));
    }

    @Test
    public void testFrameEnd100ms() {
        String[][] tests = {
                {"2022-03-14T12:23:23.345Z", "2022-03-14T12:23:23.400Z"},
                {"2022-03-14T00:00:59.982Z", "2022-03-14T00:01:00.000Z"},
                {"2022-03-14T12:59:59.999Z", "2022-03-14T13:00:00.000Z"},
                {"2022-03-14T12:00:00.000Z", "2022-03-14T12:00:00.100Z"},
                {"2022-03-14T12:00:00.101Z", "2022-03-14T12:00:00.200Z"},
                {"2022-03-14T12:00:00.099Z", "2022-03-14T12:00:00.100Z"},
                {"2022-03-14T00:00:59.000Z", "2022-03-14T00:00:59.100Z"},
        };
        for (int t = 0; t < tests.length; t++)
            assertEquals(FrameType.TYPE_100MS.getFrameEnd(Instant.parse(tests[t][0])), Instant.parse(tests[t][1]));
    }


    @Test
    public void testFrameStartSeconds() {
        String[][] tests = {
                {"2022-03-15T12:23:23.345Z", "2022-03-15T12:23:23.000Z"},
                {"2022-03-15T00:00:59.982Z", "2022-03-15T00:00:59.000Z"},
                {"2022-03-15T12:00:00.999Z", "2022-03-15T12:00:00.000Z"},
                {"2022-03-15T12:00:00.001Z", "2022-03-15T12:00:00.000Z"},
                {"2022-03-15T00:00:59.000Z", "2022-03-15T00:00:59.000Z"},
        };
        for (int t = 0; t < tests.length; t++)
            assertEquals(FrameType.TYPE_SECOND.getFrameStart(Instant.parse(tests[t][0])), Instant.parse(tests[t][1]));
    }


    @Test
    public void testFrameEndSeconds() {
        String[][] tests = {
                {"2022-03-15T12:23:23.345Z", "2022-03-15T12:23:24.000Z"},
                {"2022-03-15T00:00:59.982Z", "2022-03-15T00:01:00.000Z"},
                {"2022-03-15T23:59:59.999Z", "2022-03-16T00:00:00.000Z"},
                {"2022-03-15T12:00:00.001Z", "2022-03-15T12:00:01.000Z"},
                {"2022-03-15T00:00:59.000Z", "2022-03-15T00:01:00.000Z"},
        };
        for (int t = 0; t < tests.length; t++)
            assertEquals(FrameType.TYPE_SECOND.getFrameEnd(Instant.parse(tests[t][0])), Instant.parse(tests[t][1]));
    }


    @Test
    public void testFrameStartMinutes() {
        String[][] tests = {
                {"2022-03-15T12:23:23.345Z", "2022-03-15T12:23:00.000Z"},
                {"2022-03-15T00:59:29.982Z", "2022-03-15T00:59:00.000Z"},
                {"2022-03-15T23:59:59.999Z", "2022-03-15T23:59:00.000Z"},
                {"2022-03-15T12:00:00.001Z", "2022-03-15T12:00:00.000Z"},
                {"2022-03-15T00:00:00.000Z", "2022-03-15T00:00:00.000Z"},
        };
        for (int t = 0; t < tests.length; t++)
            assertEquals(FrameType.TYPE_MINUTE.getFrameStart(Instant.parse(tests[t][0])), Instant.parse(tests[t][1]));
    }


    @Test
    public void testFrameEndMinutes() {
        String[][] tests = {
                {"2022-03-15T12:23:23.345Z", "2022-03-15T12:24:00.000Z"},
                {"2022-03-15T00:59:29.982Z", "2022-03-15T01:00:00.000Z"},
                {"2022-03-15T23:59:59.999Z", "2022-03-16T00:00:00.000Z"},
                {"2022-03-15T12:00:00.001Z", "2022-03-15T12:01:00.000Z"},
                {"2022-03-15T00:00:00.000Z", "2022-03-15T00:01:00.000Z"},
        };
        for (int t = 0; t < tests.length; t++)
            assertEquals(FrameType.TYPE_MINUTE.getFrameEnd(Instant.parse(tests[t][0])), Instant.parse(tests[t][1]));
    }

    
    @Test
    public void testFrameStartHours() {
        String[][] tests = {
                {"2022-03-15T12:23:23.345Z", "2022-03-15T12:00:00.000Z"},
                {"2022-03-15T00:59:29.982Z", "2022-03-15T00:00:00.000Z"},
                {"2022-03-15T23:59:59.999Z", "2022-03-15T23:00:00.000Z"},
                {"2022-03-15T12:00:00.001Z", "2022-03-15T12:00:00.000Z"},
                {"2022-03-15T00:00:00.000Z", "2022-03-15T00:00:00.000Z"},
        };
        for (int t = 0; t < tests.length; t++)
            assertEquals(FrameType.TYPE_HOUR.getFrameStart(Instant.parse(tests[t][0])), Instant.parse(tests[t][1]));
    }


    @Test
    public void testFrameEndHours() {
        String[][] tests = {
                {"2022-03-15T12:23:23.345Z", "2022-03-15T13:00:00.000Z"},
                {"2022-03-15T00:59:29.982Z", "2022-03-15T01:00:00.000Z"},
                {"2022-03-15T23:59:59.999Z", "2022-03-16T00:00:00.000Z"},
                {"2022-03-15T12:00:00.001Z", "2022-03-15T13:00:00.000Z"},
                {"2022-03-15T00:00:00.000Z", "2022-03-15T01:00:00.000Z"},
        };
        for (int t = 0; t < tests.length; t++)
            assertEquals(FrameType.TYPE_HOUR.getFrameEnd(Instant.parse(tests[t][0])), Instant.parse(tests[t][1]));
    }
}

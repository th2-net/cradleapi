package com.exactpro.cradle;

import org.testng.annotations.Test;

import java.time.LocalTime;

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
        LocalTime[][] tests = {
                {LocalTime.of(12, 23, 23, 345_182_947), LocalTime.of(12, 23, 23, 300_000_000)},
                {LocalTime.of(00, 00, 59, 982_182_947), LocalTime.of(00, 00, 59, 900_000_000)},
                {LocalTime.of(12, 00, 00, 999_999_999), LocalTime.of(12, 00, 00, 900_000_000)},
                {LocalTime.of(12, 00, 00, 000_000_001), LocalTime.of(12, 00, 00, 000_000_000)},
                {LocalTime.of(12, 00, 00, 101_000_001), LocalTime.of(12, 00, 00, 100_000_000)},
                {LocalTime.of(12, 00, 00,  99_999_999), LocalTime.of(12, 00, 00, 000_000_000)},
                {LocalTime.of(00, 00, 59, 000_000_000), LocalTime.of(00, 00, 59, 000_000_000)},
        };
        for (int t = 0; t < tests.length; t++)
            assertEquals(FrameType.TYPE_100MS.getFrameStart(tests[t][0]), tests[t][1]);
    }

    @Test
    public void testFrameEnd100ms() {
        LocalTime[][] tests = {
                {LocalTime.of(12, 23, 23, 345_182_947), LocalTime.of(12, 23, 23, 400_000_000)},
                {LocalTime.of(00, 00, 59, 982_182_947), LocalTime.of(00, 01, 00, 000_000_000)},
                {LocalTime.of(12, 59, 59, 999_999_999), LocalTime.of(13, 00, 00, 000_000_000)},
                {LocalTime.of(12, 00, 00, 000_000_001), LocalTime.of(12, 00, 00, 100_000_000)},
                {LocalTime.of(12, 00, 00, 101_000_001), LocalTime.of(12, 00, 00, 200_000_000)},
                {LocalTime.of(12, 00, 00,  99_999_999), LocalTime.of(12, 00, 00, 100_000_000)},
                {LocalTime.of(00, 00, 59, 000_000_000), LocalTime.of(00, 00, 59, 100_000_000)},
        };
        for (int t = 0; t < tests.length; t++)
            assertEquals(FrameType.TYPE_100MS.getFrameEnd(tests[t][0]), tests[t][1]);
    }


    @Test
    public void testFrameStartSeconds() {
        LocalTime[][] tests = {
                {LocalTime.of(12, 23, 23, 345_182_947), LocalTime.of(12, 23, 23, 0)},
                {LocalTime.of(00, 00, 59, 982_182_947), LocalTime.of(00, 00, 59, 0)},
                {LocalTime.of(12, 00, 00, 999_999_999), LocalTime.of(12, 00, 00, 0)},
                {LocalTime.of(12, 00, 00, 000_000_001), LocalTime.of(12, 00, 00, 0)},
                {LocalTime.of(00, 00, 59, 000_000_000), LocalTime.of(00, 00, 59, 0)},
        };
        for (int t = 0; t < tests.length; t++)
            assertEquals(FrameType.TYPE_SECOND.getFrameStart(tests[t][0]), tests[t][1]);
    }

    @Test
    public void testFrameEndSeconds() {
        LocalTime[][] tests = {
                {LocalTime.of(12, 23, 23, 345_182_947), LocalTime.of(12, 23, 24, 0)},
                {LocalTime.of(00, 00, 59, 982_182_947), LocalTime.of(00, 01, 00, 0)},
                {LocalTime.of(23, 59, 59, 999_999_999), LocalTime.of(00, 00, 00, 0)},
                {LocalTime.of(12, 00, 00, 000_000_001), LocalTime.of(12, 00, 01, 0)},
                {LocalTime.of(00, 00, 59, 000_000_000), LocalTime.of(00, 01, 00, 0)},
        };
        for (int t = 0; t < tests.length; t++)
            assertEquals(FrameType.TYPE_SECOND.getFrameEnd(tests[t][0]), tests[t][1]);
    }


    @Test
    public void testFrameStartMinutes() {
        LocalTime[][] tests = {
                {LocalTime.of(12, 23, 23, 345_182_947), LocalTime.of(12, 23, 00, 0)},
                {LocalTime.of(00, 59, 29, 982_182_947), LocalTime.of(00, 59, 00, 0)},
                {LocalTime.of(23, 59, 59, 999_999_999), LocalTime.of(23, 59, 00, 0)},
                {LocalTime.of(12, 00, 00, 000_000_001), LocalTime.of(12, 00, 00, 0)},
                {LocalTime.of(00, 00, 00, 000_000_000), LocalTime.of(00, 00, 00, 0)},
        };
        for (int t = 0; t < tests.length; t++)
            assertEquals(FrameType.TYPE_MINUTE.getFrameStart(tests[t][0]), tests[t][1]);
    }


    @Test
    public void testFrameEndMinutes() {
        LocalTime[][] tests = {
                {LocalTime.of(12, 23, 23, 345_182_947), LocalTime.of(12, 24, 00, 0)},
                {LocalTime.of(00, 59, 29, 982_182_947), LocalTime.of(01, 00, 00, 0)},
                {LocalTime.of(23, 59, 59, 999_999_999), LocalTime.of(00, 00, 00, 0)},
                {LocalTime.of(12, 00, 00, 000_000_001), LocalTime.of(12, 01, 00, 0)},
                {LocalTime.of(00, 00, 00, 000_000_000), LocalTime.of(00, 01, 00, 0)},
        };
        for (int t = 0; t < tests.length; t++)
            assertEquals(FrameType.TYPE_MINUTE.getFrameEnd(tests[t][0]), tests[t][1]);
    }

    @Test
    public void testFrameStartHours() {
        LocalTime[][] tests = {
                {LocalTime.of(12, 23, 23, 345_182_947), LocalTime.of(12, 00, 00, 0)},
                {LocalTime.of(00, 59, 29, 982_182_947), LocalTime.of(00, 00, 00, 0)},
                {LocalTime.of(23, 59, 59, 999_999_999), LocalTime.of(23, 00, 00, 0)},
                {LocalTime.of(12, 00, 00, 000_000_001), LocalTime.of(12, 00, 00, 0)},
                {LocalTime.of(00, 00, 00, 000_000_000), LocalTime.of(00, 00, 00, 0)},
        };
        for (int t = 0; t < tests.length; t++)
            assertEquals(FrameType.TYPE_HOUR.getFrameStart(tests[t][0]), tests[t][1]);
    }


    @Test
    public void testFrameEndHours() {
        LocalTime[][] tests = {
                {LocalTime.of(12, 23, 23, 345_182_947), LocalTime.of(13, 00, 00, 0)},
                {LocalTime.of(00, 59, 29, 982_182_947), LocalTime.of(01, 00, 00, 0)},
                {LocalTime.of(23, 59, 59, 999_999_999), LocalTime.of(00, 00, 00, 0)},
                {LocalTime.of(12, 00, 00, 000_000_001), LocalTime.of(13, 00, 00, 0)},
                {LocalTime.of(00, 00, 00, 000_000_000), LocalTime.of(01, 00, 00, 0)},
        };
        for (int t = 0; t < tests.length; t++)
            assertEquals(FrameType.TYPE_HOUR.getFrameEnd(tests[t][0]), tests[t][1]);
    }
}

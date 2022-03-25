package com.exactpro.cradle.cassandra;

import com.exactpro.cradle.FrameType;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.testng.annotations.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.testng.Assert.*;

public class CassandraCradleStorageTest {

    @Test
    private void testEmptyInterval () {
        Instant start, end;
        start = Instant.parse("2022-03-22T17:05:37.113Z");
        end  = Instant.parse("2022-03-22T17:05:37.116Z");

        List<ImmutableTriple<FrameType, Instant, Instant>> actual, expected;
        actual = CassandraCradleStorage.sliceInterval(start, end);
        expected = new ArrayList<>();

        assertEquals(actual, expected);
    }

    @Test
    public void testOnlyMillisecondFramesInterval () {
        Instant start, end;
        start = Instant.parse("2022-03-22T17:05:37.100Z");
        end  = Instant.parse("2022-03-22T17:05:38.596Z");

        List<ImmutableTriple<FrameType, Instant, Instant>> actual, expected;
        actual = CassandraCradleStorage.sliceInterval(start, end);
        expected = new ArrayList<>();
        expected.add(new ImmutableTriple<>(FrameType.TYPE_100MS,
                Instant.parse("2022-03-22T17:05:37.100Z"),
                Instant.parse("2022-03-22T17:05:38.000Z")));
        expected.add(new ImmutableTriple<>(FrameType.TYPE_100MS,
                Instant.parse("2022-03-22T17:05:38.000Z"),
                Instant.parse("2022-03-22T17:05:38.500Z")));

        assertEquals(actual, expected);
    }

    @Test
    public void testOnlySecondFramesInterval () {
        Instant start, end;
        start = Instant.parse("2022-03-22T17:05:25.000Z");
        end  = Instant.parse("2022-03-22T17:06:59.000Z");

        List<ImmutableTriple<FrameType, Instant, Instant>> actual, expected;
        actual = CassandraCradleStorage.sliceInterval(start, end);
        expected = new ArrayList<>();
        expected.add(new ImmutableTriple<>(FrameType.TYPE_SECOND,
                Instant.parse("2022-03-22T17:05:25.000Z"),
                Instant.parse("2022-03-22T17:06:00.000Z")));
        expected.add(new ImmutableTriple<>(FrameType.TYPE_SECOND,
                Instant.parse("2022-03-22T17:06:00.000Z"),
                Instant.parse("2022-03-22T17:06:59.000Z")));

        assertEquals(actual, expected);
    }

    @Test
    public void testOnlyMinuteFramesInterval () {
        Instant start, end;
        start = Instant.parse("2022-03-22T17:05:00.000Z");
        end  = Instant.parse("2022-03-22T18:23:00.000Z");

        List<ImmutableTriple<FrameType, Instant, Instant>> actual, expected;
        actual = CassandraCradleStorage.sliceInterval(start, end);
        expected = new ArrayList<>();
        expected.add(new ImmutableTriple<>(FrameType.TYPE_MINUTE,
                Instant.parse("2022-03-22T17:05:00.000Z"),
                Instant.parse("2022-03-22T18:00:00.000Z")));
        expected.add(new ImmutableTriple<>(FrameType.TYPE_MINUTE,
                Instant.parse("2022-03-22T18:00:00.000Z"),
                Instant.parse("2022-03-22T18:23:00.000Z")));

        assertEquals(actual, expected);
    }

    @Test
    public void testOnlyHoursFramesInterval () {
        Instant start, end;
        start = Instant.parse("2022-03-22T17:00:00.025Z");
        end  = Instant.parse("2022-03-24T20:00:00.420Z");

        List<ImmutableTriple<FrameType, Instant, Instant>> actual, expected;
        actual = CassandraCradleStorage.sliceInterval(start, end);
        expected = new ArrayList<>();
        expected.add(new ImmutableTriple<>(FrameType.TYPE_HOUR,
                Instant.parse("2022-03-22T17:00:00.000Z"),
                Instant.parse("2022-03-24T20:00:00.000Z")));

        assertEquals(actual, expected);
    }

    @Test
    public void testUptoSecondFramesInterval () {
        Instant start, end;
        start = Instant.parse("2022-03-22T17:05:37.128Z");
        end  = Instant.parse("2022-03-22T17:05:40.250Z");

        List<ImmutableTriple<FrameType, Instant, Instant>> actual, expected;
        actual = CassandraCradleStorage.sliceInterval(start, end);
        expected = new ArrayList<>();
        expected.add(new ImmutableTriple<>(FrameType.TYPE_100MS,
                Instant.parse("2022-03-22T17:05:37.100Z"),
                Instant.parse("2022-03-22T17:05:38.000Z")));
        expected.add(new ImmutableTriple<>(FrameType.TYPE_SECOND,
                Instant.parse("2022-03-22T17:05:38.000Z"),
                Instant.parse("2022-03-22T17:05:40.000Z")));
        expected.add(new ImmutableTriple<>(FrameType.TYPE_100MS,
                Instant.parse("2022-03-22T17:05:40.000Z"),
                Instant.parse("2022-03-22T17:05:40.200Z")));

        assertEquals(actual, expected);
    }

    @Test
    public void testUptoMinuteFramesInterval () {
        Instant start, end;
        start = Instant.parse("2022-03-22T17:05:37.128Z");
        end  = Instant.parse("2022-03-22T17:15:40.250Z");

        List<ImmutableTriple<FrameType, Instant, Instant>> actual, expected;
        actual = CassandraCradleStorage.sliceInterval(start, end);
        expected = new ArrayList<>();
        expected.add(new ImmutableTriple<>(FrameType.TYPE_100MS,
                Instant.parse("2022-03-22T17:05:37.100Z"),
                Instant.parse("2022-03-22T17:05:38.000Z")));
        expected.add(new ImmutableTriple<>(FrameType.TYPE_SECOND,
                Instant.parse("2022-03-22T17:05:38.000Z"),
                Instant.parse("2022-03-22T17:06:00.000Z")));
        expected.add(new ImmutableTriple<>(FrameType.TYPE_MINUTE,
                Instant.parse("2022-03-22T17:06:00.000Z"),
                Instant.parse("2022-03-22T17:15:00.000Z")));
        expected.add(new ImmutableTriple<>(FrameType.TYPE_SECOND,
                Instant.parse("2022-03-22T17:15:00.000Z"),
                Instant.parse("2022-03-22T17:15:40.000Z")));
        expected.add(new ImmutableTriple<>(FrameType.TYPE_100MS,
                Instant.parse("2022-03-22T17:15:40.000Z"),
                Instant.parse("2022-03-22T17:15:40.200Z")));

        assertEquals(actual, expected);
    }

    @Test
    public void testUptoHourFramesInterval () {

        final Instant start = Instant.parse("2022-03-22T17:05:37.128Z");
        final Instant end   = Instant.parse("2022-03-22T20:15:40.250Z");

        final List<ImmutableTriple<FrameType, Instant, Instant>> actual = CassandraCradleStorage.sliceInterval(start, end);
        validateSlices(actual, start, end, 7);
    }


    private void validateSlices(List<ImmutableTriple<FrameType, Instant, Instant>> actual,
                                Instant start,
                                Instant end,
                                int expectedSize) {

        final int size = actual.size();
        final FrameType minFrame = FrameType.values()[0];

        assertEquals(actual.get(0).getMiddle(), minFrame.getFrameStart(start), "Wrong start time of the series:");
        assertEquals(actual.get(size - 1).getRight(), minFrame.getFrameEnd(end), "Wrong end time of the series:");
        assertEquals(size, expectedSize, "Wrong number of intervals:");
        validateContinuity(actual);
    }


    private void validateInterval(ImmutableTriple<FrameType, Instant, Instant> interval) {
        FrameType frameType = interval.getLeft();
        Instant start = interval.getMiddle();
        Instant end = interval.getRight();
        if (!start.isBefore(end))
            fail(String.format("Interval start must be strictly before its end: start=%s, end=%s", start, end));

        assertEquals(start, frameType.getFrameStart(start), String.format("Interval start does not align to FrameType.%s start:", frameType));
        assertEquals(start, frameType.getFrameStart(start), String.format("Interval end does not align to FrameType.%s end:", frameType));
    }


    private void validateContinuity(List<ImmutableTriple<FrameType, Instant, Instant>> intervals) {
        ImmutableTriple<FrameType, Instant, Instant> last = null;
        for (ImmutableTriple<FrameType, Instant, Instant> el : intervals) {
            validateInterval(el);
            if (last != null) {
                assertNotEquals(el.getLeft(), last.getLeft(), String.format("Identical frame types[%s] on adjacent elements", el.getLeft()));
                assertEquals(el.getMiddle(), last.getRight(), "Sequence continuity failure:");
            }
            last = el;
        }
    }
}

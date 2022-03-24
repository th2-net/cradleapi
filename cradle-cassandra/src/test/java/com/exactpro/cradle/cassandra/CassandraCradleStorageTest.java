package com.exactpro.cradle.cassandra;

import com.exactpro.cradle.FrameType;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.testng.annotations.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class CassandraCradleStorageTest {

    @Test
    private void testEmptyInterval () {
        Instant start, end;
        start = Instant.parse("2022-03-22T17:05:37.113Z");
        end  = Instant.parse("2022-03-22T17:05:37.116Z");

        List<ImmutableTriple<FrameType, Instant, Instant>> actual, expected;
        actual = CassandraCradleStorage.sliceInterval(start, end);
        expected = new ArrayList<>();

        assert actual.equals(expected);
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

        assert actual.equals(expected);
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

        assert actual.equals(expected);
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

        assert actual.equals(expected);
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

        assert actual.equals(expected);
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

        assert actual.equals(expected);
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

        assert actual.equals(expected);
    }

    @Test
    public void testUptoHourFramesInterval () {
        Instant start, end;
        start = Instant.parse("2022-03-22T17:05:37.128Z");
        end  = Instant.parse("2022-03-22T20:15:40.250Z");

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
                Instant.parse("2022-03-22T18:00:00.000Z")));
        expected.add(new ImmutableTriple<>(FrameType.TYPE_HOUR,
                Instant.parse("2022-03-22T18:00:00.000Z"),
                Instant.parse("2022-03-22T20:00:00.000Z")));
        expected.add(new ImmutableTriple<>(FrameType.TYPE_MINUTE,
                Instant.parse("2022-03-22T20:00:00.000Z"),
                Instant.parse("2022-03-22T20:15:00.000Z")));
        expected.add(new ImmutableTriple<>(FrameType.TYPE_SECOND,
                Instant.parse("2022-03-22T20:15:00.000Z"),
                Instant.parse("2022-03-22T20:15:40.000Z")));
        expected.add(new ImmutableTriple<>(FrameType.TYPE_100MS,
                Instant.parse("2022-03-22T20:15:40.000Z"),
                Instant.parse("2022-03-22T20:15:40.200Z")));

        assert actual.equals(expected);
    }
}

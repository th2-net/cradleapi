package com.exactpro.cradle.cassandra.utils;

import com.exactpro.cradle.FrameType;
import com.exactpro.cradle.cassandra.counters.FrameInterval;
import com.exactpro.cradle.counters.Interval;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class StorageUtils {

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
}

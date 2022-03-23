package com.exactpro.cradle;

import java.time.Instant;

public class CounterSample {

    private final Instant frameStart;
    private final Counter counter;
    private final FrameType frameType;

    public CounterSample(FrameType frameType, Instant frameStart, Counter counter) {
        this.frameType = frameType;
        this.frameStart = frameStart;
        this.counter = counter;
    }

    public Instant getFrameStart() {
        return frameStart;
    }

    public Counter getCounter() {
        return counter;
    }

    public FrameType getFrameType() {
        return frameType;
    }
}

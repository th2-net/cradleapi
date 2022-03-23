package com.exactpro.cradle;

import java.time.Instant;

public class CounterSample {

    private final Instant frameStart;
    private final Counter counter;
    private final FrameType frameType;

    public CounterSample(Instant frameStart, Counter counter, FrameType frameType) {
        this.frameStart = frameStart;
        this.counter = counter;
        this.frameType = frameType;
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

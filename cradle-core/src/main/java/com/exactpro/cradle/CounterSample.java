package com.exactpro.cradle;

import java.time.Instant;

public class CounterSample {

    private final Instant frameStart;
    private final Counter counter;

    CounterSample(Instant frameStart, Counter counter) {
        this.frameStart = frameStart;
        this.counter = counter;
    }

    public Instant getFrameStart() {
        return frameStart;
    }

    public Counter getCounter() {
        return counter;
    }
}

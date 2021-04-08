package com.exactpro.cradle.healing;

//TODO: add more properties if necessary

import java.time.Instant;

public class HealingInterval
{
    private final String id;
    private final Instant startTime;
    private final Instant endTime;

    public HealingInterval(String id, Instant startTime, Instant endTime)
    {
        this.id = id;
        this.startTime = startTime;
        this.endTime = endTime;
    }

    public String getId() { return id; }

    public Instant getStartTime() { return startTime; }

    public Instant getEndTime() { return endTime; }
}

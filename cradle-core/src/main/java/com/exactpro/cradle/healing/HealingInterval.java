package com.exactpro.cradle.healing;

//TODO: add more properties if necessary

import java.time.Instant;

public class HealingInterval
{
    private final String id;
    private final Instant startTime;
    private final long maxLength;
    private final RecoveryState recoveryState;

    public HealingInterval(String id, Instant startTime, long maxLength, RecoveryState recoveryState)
    {
        this.id = id;
        this.startTime = startTime;
        this.maxLength = maxLength;
        this.recoveryState = recoveryState;
    }

    public String getId() { return id; }

    public Instant getStartTime() { return startTime; }

    public long getMaxLength() { return maxLength; }

    public RecoveryState getRecoveryState() { return recoveryState; }
}

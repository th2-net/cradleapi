package com.exactpro.cradle.healing;

//TODO: add more properties if necessary

import com.exactpro.cradle.utils.CompressionUtils;

import java.time.LocalTime;

public class HealingInterval
{
    private final String id;
    private final LocalTime startTime;
    private final LocalTime endTime;
    private final RecoveryState recoveryState;

    public HealingInterval(String id, LocalTime startTime, LocalTime endTime, RecoveryState recoveryState)
    {
        this.id = id;
        this.startTime = startTime;
        this.endTime = endTime;
        this.recoveryState = recoveryState;
    }

    public String getId() { return id; }

    public LocalTime getStartTime() { return startTime; }

    public LocalTime getEndTime() { return endTime; }

    public RecoveryState getRecoveryState() { return recoveryState; }

    @Override
    public String toString()
    {
        return new StringBuilder()
                .append("HealingInterval{").append(CompressionUtils.EOL)
                .append("id=").append(id).append(",").append(CompressionUtils.EOL)
                .append("startTime=").append(startTime).append(",").append(CompressionUtils.EOL)
                .append("endTime=").append(endTime).append(",").append(CompressionUtils.EOL)
                .append(recoveryState.toString()).append(CompressionUtils.EOL)
                .append("}").toString();
    }
}

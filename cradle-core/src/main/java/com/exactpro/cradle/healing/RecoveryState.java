package com.exactpro.cradle.healing;

//TODO: add more properties if necessary

import com.exactpro.cradle.utils.CompressionUtils;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Arrays;

public class RecoveryState
{
    private final String id;

    private final long healedEventsNumber;

    public RecoveryState(@JsonProperty("id") String id, @JsonProperty("healedEventsNumber") long healedEventsNumber) {
        this.id = id;
        this.healedEventsNumber = healedEventsNumber;
    }

    public String getId() { return id; }

    public long getHealedEventsNumber() { return healedEventsNumber; }

    @Override
    public String toString()
    {
        return new StringBuilder()
                .append("RecoveryState{")
                .append("id=").append(id).append(",")
                .append("healedEventsNumber=").append(healedEventsNumber)
                .append("}").toString();
    }
}

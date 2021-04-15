package com.exactpro.cradle.healing;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.LocalTime;

public class RecoveryState
{
    private final String id;

    private final long healedEventsNumber;

    private final LocalTime timeOfStop;

    public RecoveryState(@JsonProperty("id") String id, @JsonProperty("healedEventsNumber") long healedEventsNumber, @JsonProperty("timeOfStop") LocalTime timeOfStop) {
        this.id = id;
        this.healedEventsNumber = healedEventsNumber;
        this.timeOfStop = timeOfStop;
    }

    public String getId() { return id; }

    public long getHealedEventsNumber() { return healedEventsNumber; }

    public LocalTime getTimeOfStop() { return timeOfStop; }

    @Override
    public String toString()
    {
        return new StringBuilder()
                .append("RecoveryState{")
                .append("id=").append(id).append(",")
                .append("healedEventsNumber=").append(healedEventsNumber)
                .append("timeOfStop=").append(timeOfStop)
                .append("}").toString();
    }
}

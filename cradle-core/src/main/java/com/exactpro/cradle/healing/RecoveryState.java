package com.exactpro.cradle.healing;

//TODO: add more properties if necessary

public class RecoveryState
{
    private final String id;

    private final long healedEventsNumber;

    public RecoveryState(String id, long healedEventsNumber) {
        this.id = id;
        this.healedEventsNumber = healedEventsNumber;
    }

    public String getId() { return id; }

    public long getHealedEventsNumber() { return healedEventsNumber; }
}

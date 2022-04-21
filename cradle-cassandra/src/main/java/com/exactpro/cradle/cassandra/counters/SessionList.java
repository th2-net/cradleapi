package com.exactpro.cradle.cassandra.counters;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;

public class SessionList {
    private final HashSet<String> sessionSet;
    private final String session;

    // SessionList will store single session in String field
    // and for multiple sessions will utilize Set
    public SessionList(String session) {
        this.sessionSet = null;
        this.session = session;
    }

    private SessionList(HashSet<String> sessionSet) {
        this.sessionSet = sessionSet;
        this.session = null;
    }

    private static void addSessionList(HashSet<String> set, SessionList sessionList) {
        if (sessionList.sessionSet != null)
            set.addAll(sessionList.sessionSet);
        else
            set.add(sessionList.session);
    }

    public Collection<String> getSessions() {
        if (session != null)
            return List.of(session);
        return sessionSet;
    }

    public SessionList mergedWith(SessionList sessionList) {
        if (containsAll(sessionList))
            return this;

        HashSet<String> set;
        if (this.sessionSet != null)
            set = (HashSet<String>) sessionSet.clone();
        else {
            set = new HashSet<>();
            set.add(this.session);
        }
        addSessionList(set, sessionList);

        return new SessionList(set);
    }

    public boolean containsAll(SessionList other) {
        // if we hold single session
        if (this.session != null) {
            if (this.session.equals(other.session))
                return true;
            if (other.sessionSet != null && other.sessionSet.size() == 1 && other.sessionSet.contains(this.session))
                return true;
        } else {
            // if we hold multiple sessions
            if (other.session != null)
                return this.sessionSet.contains(other.session);
            return this.sessionSet.containsAll(other.sessionSet);
        }

        return false;
    }
}
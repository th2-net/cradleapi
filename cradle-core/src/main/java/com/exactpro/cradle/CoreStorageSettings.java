package com.exactpro.cradle;

public class CoreStorageSettings {
    protected static final long DEFAULT_BOOK_REFRESH_INTERVAL_MILLIS = 60000;
    protected long bookRefreshIntervalMillis = DEFAULT_BOOK_REFRESH_INTERVAL_MILLIS;

    public long getBookRefreshIntervalMillis() {
        return bookRefreshIntervalMillis;
    }

    public void setBookRefreshIntervalMillis(long bookRefreshIntervalMillis) {
        this.bookRefreshIntervalMillis = bookRefreshIntervalMillis;
    }

    public long calculatePageActionRejectionThreshold(){
        return this.bookRefreshIntervalMillis * 2;
    }

    public long calculateStoreActionRejectionThreshold(){
        return this.bookRefreshIntervalMillis;
    }
}

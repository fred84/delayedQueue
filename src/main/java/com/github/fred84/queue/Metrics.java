package com.github.fred84.queue;

public interface Metrics {

    <T extends Event> void incrementCounterFor(Class<T> type, String direction);

    //void reportScheduledCount(long count);

    //<T extends Event> void reportReadyForHandling(Class<T> type, long count);
}

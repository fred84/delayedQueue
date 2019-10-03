package com.github.fred84.queue;

public interface Metrics {

    <T extends Event> void incrementCounterFor(Class<T> type, String direction);
}

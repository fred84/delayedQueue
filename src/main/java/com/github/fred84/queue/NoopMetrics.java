package com.github.fred84.queue;

public class NoopMetrics implements Metrics {

    @Override
    public <T extends Event> void incrementCounterFor(Class<T> type, String direction) {
    }
}

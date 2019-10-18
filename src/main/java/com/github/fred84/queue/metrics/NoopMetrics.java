package com.github.fred84.queue.metrics;

import com.github.fred84.queue.Event;
import java.util.function.Supplier;

public class NoopMetrics implements Metrics {

    @Override
    public <T extends Event> void incrementCounterFor(Class<T> type, String direction) {
    }

    @Override
    public void registerScheduledCountSupplier(Supplier<Number> countSupplier) {

    }

    @Override
    public <T extends Event> void registerReadyToProcessSupplier(Class<T> type, Supplier<Number> countSupplier) {

    }
}

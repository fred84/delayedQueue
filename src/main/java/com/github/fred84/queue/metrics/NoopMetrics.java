package com.github.fred84.queue.metrics;

import com.github.fred84.queue.Event;
import java.util.function.Supplier;

public class NoopMetrics implements Metrics {

    @Override
    public <T extends Event> void incrementEnqueueCounter(Class<T> type) {
        // no-op
    }

    @Override
    public <T extends Event> void incrementDequeueCounter(Class<T> type) {
        // no-op
    }

    @Override
    public void registerScheduledCountSupplier(Supplier<Number> countSupplier) {
        // no-op
    }

    @Override
    public <T extends Event> void registerReadyToProcessSupplier(Class<T> type, Supplier<Number> countSupplier) {
        // no-op
    }
}

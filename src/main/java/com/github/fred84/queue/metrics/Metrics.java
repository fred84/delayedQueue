package com.github.fred84.queue.metrics;

import com.github.fred84.queue.Event;
import java.util.function.Supplier;

public interface Metrics {

    <T extends Event> void incrementCounterFor(Class<T> type, String direction);

    void registerScheduledCountSupplier(Supplier<Number> countSupplier);

    <T extends Event> void registerReadyToProcessSupplier(Class<T> type, Supplier<Number> countSupplier);
}

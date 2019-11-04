package com.github.fred84.queue.metrics;

import com.github.fred84.queue.Event;
import java.util.function.Supplier;

public interface Metrics {

    <T extends Event> void incrementEnqueueCounter(Class<T> type);

    <T extends Event> void incrementDequeueCounter(Class<T> type);

    void registerScheduledCountSupplier(Supplier<Number> countSupplier);

    <T extends Event> void registerReadyToProcessSupplier(Class<T> type, Supplier<Number> countSupplier);
}

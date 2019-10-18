package com.github.fred84.queue.metrics;

import com.github.fred84.queue.Event;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

public class MicrometerMetrics implements Metrics {

    private final Map<Class<? extends Event>, Map<String, Counter>> counters = new ConcurrentHashMap<>();
    private final MeterRegistry registry;

    public MicrometerMetrics(MeterRegistry registry) {
        this.registry = registry;
    }

    @Override
    public <T extends Event> void incrementCounterFor(Class<T> type, String direction) {
        getCounter(type, direction).increment();
    }

    @Override
    public void registerScheduledCountSupplier(Supplier<Number> countSupplier) {
        Gauge.builder("scheduled.count", countSupplier)
                .register(registry);
    }

    @Override
    public <T extends Event> void registerReadyToProcessSupplier(Class<T> type, Supplier<Number> countSupplier) {
        Gauge.builder("ready.for.handling.queue", countSupplier)
                .tag("type", type.getSimpleName())
                .register(registry);
    }

    private <T extends Event> Counter getCounter(Class<T> type, String direction) {
        return counters
                .computeIfAbsent(type, clz -> new ConcurrentHashMap<>())
                .computeIfAbsent(direction, dir -> Counter
                        .builder("delayed.queue.events")
                        .tag("type", type.getSimpleName())
                        .tag("direction", direction)
                        .register(registry)
                );
    }
}

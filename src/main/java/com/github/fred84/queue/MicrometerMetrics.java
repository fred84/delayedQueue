package com.github.fred84.queue;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

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

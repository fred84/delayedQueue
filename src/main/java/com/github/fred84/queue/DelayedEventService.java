package com.github.fred84.queue;

import java.time.Duration;
import java.util.function.Function;
import reactor.core.publisher.Mono;

public interface DelayedEventService {

    void enqueueWithDelay(Event event, Duration delay);

    <T extends Event> void addBlockingHandler(Class<T> eventType, Function<T, Boolean> handler, int parallelism);

    <T extends Event> void addHandler(Class<T> eventType, Function<T, Mono<Boolean>> handler, int parallelism);

    <T extends Event> boolean removeHandler(Class<T> eventType);
}
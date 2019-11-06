package com.github.fred84.queue;

import java.util.function.Function;
import java.util.function.Predicate;
import reactor.core.publisher.Mono;

class BlockingSubscriber<T extends Event> implements Function<T, Mono<Boolean>> {

    private final Predicate<T> handler;

    BlockingSubscriber(Predicate<T> handler) {
        this.handler = handler;
    }

    @Override
    public Mono<Boolean> apply(T event) {
        if (event == null) {
            return null;
        }

        try {
            return Mono.fromSupplier(() -> handler.test(event));
        } catch (Exception e) {
            return Mono.just(false);
        }
    }
}

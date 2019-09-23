package com.github.fred84.queue;

import java.util.function.Function;
import reactor.core.publisher.Mono;

class BlockingSubscriber<T extends Event> implements Function<T, Mono<Boolean>> {

    private final Function<T, Boolean> handler;

    BlockingSubscriber(Function<T, Boolean> handler) {
        this.handler = handler;
    }

    @Override
    public Mono<Boolean> apply(T event) {
        if (event == null) {
            return null;
        }

        try {
            return Mono.fromSupplier(() -> handler.apply(event));
        } catch (Exception e) {
            return Mono.just(false);
        }
    }
}

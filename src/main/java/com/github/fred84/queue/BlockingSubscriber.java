package com.github.fred84.queue;

import static java.util.Collections.emptyMap;

import com.github.fred84.queue.logging.EventContextHandler;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;
import org.slf4j.MDC;
import reactor.core.publisher.Mono;

class BlockingSubscriber<T extends Event> implements Function<T, Mono<Boolean>> {

    private final Predicate<T> handler;
    private final EventContextHandler contextHandler;

    BlockingSubscriber(Predicate<T> handler, EventContextHandler contextHandler) {
        this.handler = handler;
        this.contextHandler = contextHandler;
    }

    @Override
    public Mono<Boolean> apply(T event) {
        if (event == null) {
            return null;
        }

        return Mono.subscriberContext().map(ctx -> {
            Map<String, String> currentMDC = mdcContext();

            try {
                contextHandler.applyToMDC(ctx);
                return handler.test(event);
            } catch (Exception e) {
                return false;
            } finally {
                MDC.setContextMap(currentMDC);
            }
        });
    }

    private Map<String, String> mdcContext() {
        Map<String, String> context = MDC.getCopyOfContextMap();

        return context == null ? emptyMap() : context;
    }
}

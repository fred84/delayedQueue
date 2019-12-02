package com.github.fred84.queue;

import static java.lang.Boolean.TRUE;

import com.github.fred84.queue.logging.LogContext;
import io.lettuce.core.TransactionResult;
import io.lettuce.core.api.StatefulRedisConnection;
import java.util.Map;
import java.util.function.Function;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

class InnerSubscriber<T extends Event> extends BaseSubscriber<EventEnvelope<T>> {

    private static final Logger LOG = LoggerFactory.getLogger(InnerSubscriber.class);

    private final LogContext logContext;
    private final Function<T, Mono<Boolean>> handler;
    private final int parallelism;
    private final StatefulRedisConnection<String, String> pollingConnection;
    private final Scheduler handlerScheduler;
    private final Function<Event, Mono<TransactionResult>> deleteCommand;

    InnerSubscriber(
            LogContext logContext,
            Function<T, Mono<Boolean>> handler,
            int parallelism,
            StatefulRedisConnection<String, String> pollingConnection,
            Scheduler handlerScheduler,
            Function<Event, Mono<TransactionResult>> deleteCommand
    ) {
        this.logContext = logContext;
        this.handler = handler;
        this.parallelism = parallelism;
        this.pollingConnection = pollingConnection;
        this.handlerScheduler = handlerScheduler;
        this.deleteCommand = deleteCommand;
    }

    @Override
    protected void hookOnSubscribe(Subscription subscription) {
        requestInner(parallelism);
    }

    @Override
    protected void hookOnNext(EventEnvelope<T> envelope) {
        LOG.debug("event [{}] received from queue", envelope);

        Mono<Boolean> promise;

        Map<String, String> originalLogContext = logContext.get();
        try {
            logContext.set(envelope.getLogContext());
            promise = handler.apply(envelope.getPayload());
            logContext.set(originalLogContext);
        } catch (Exception e) {
            logContext.set(originalLogContext);
            LOG.info("error in non-blocking handler for [{}]", envelope.getType(), e);
            requestInner(1);
            return;
        }

        if (promise == null) {
            requestInner(1);
            return;
        }

        promise
                .doFirst(() -> logContext.set(envelope.getLogContext()))
                .defaultIfEmpty(false)
                .doOnError(e -> LOG.warn("error occurred during handling event [{}]", envelope, e))
                .onErrorReturn(false)
                .flatMap(completed -> {
                    if (TRUE.equals(completed)) {
                        LOG.debug("deleting event {} from delayed queue", envelope.getPayload());
                        // todo we could also fail here!!! test me! with latch and toxyproxy
                        return deleteCommand.apply(envelope.getPayload()).map(r -> true);
                    } else {
                        return Mono.just(true);
                    }
                })
                .subscribeOn(handlerScheduler)
                .subscribe(r -> {
                    LOG.debug("event processing completed [{}]", envelope);
                    requestInner(1);
                });
    }

    @Override
    protected void hookOnCancel() {
        pollingConnection.close();
        LOG.debug("subscription connection shut down");
    }

    private void requestInner(long n) {
        LOG.debug("requesting next {} elements", n);
        request(n);
    }

    @Override
    public void dispose() {
        pollingConnection.close();
        super.dispose();
    }
}

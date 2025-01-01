package com.github.fred84.queue;

import static java.lang.Boolean.TRUE;

import com.github.fred84.queue.context.EventContextHandler;
import io.lettuce.core.TransactionResult;
import io.lettuce.core.api.StatefulRedisConnection;
import java.util.function.Function;

import org.jetbrains.annotations.NotNull;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

class InnerSubscriber<T extends Event> extends BaseSubscriber<EventEnvelope<T>> {

    private static final Logger LOG = LoggerFactory.getLogger(InnerSubscriber.class);

    private final EventContextHandler contextHandler;
    private final Function<T, Mono<Boolean>> handler;
    private final int parallelism;
    private final StatefulRedisConnection<String, String> pollingConnection;
    private final Scheduler handlerScheduler;
    private final Function<Event, Mono<TransactionResult>> deleteCommand;

    InnerSubscriber(
            EventContextHandler contextHandler,
            Function<T, Mono<Boolean>> handler,
            int parallelism,
            StatefulRedisConnection<String, String> pollingConnection,
            Scheduler handlerScheduler,
            Function<Event, Mono<TransactionResult>> deleteCommand
    ) {
        this.contextHandler = contextHandler;
        this.handler = handler;
        this.parallelism = parallelism;
        this.pollingConnection = pollingConnection;
        this.handlerScheduler = handlerScheduler;
        this.deleteCommand = deleteCommand;
    }

    @Override
    protected void hookOnSubscribe(@NotNull Subscription subscription) {
        requestInner(parallelism);
    }

    @Override
    protected void hookOnNext(@NotNull EventEnvelope<T> envelope) {
        LOG.debug("event [{}] received from queue", envelope);

        Mono<Boolean> promise;

        try {
            promise = handler.apply(envelope.getPayload());
        } catch (Exception e) {
            LOG.info("error in non-blocking handler for [{}]", envelope.getType(), e);
            requestInner(1);
            return;
        }

        if (promise == null) {
            requestInner(1);
            return;
        }

        promise
                .defaultIfEmpty(false)
                // todo think how to inject log context enricher (e.g. what fields to set in logs)

                // todo think of introducing an error which will terminate execution gracefully (e.g. no need to fail hard)
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
<<<<<<< Updated upstream
                .subscriberContext(c -> contextHandler.subscriptionContext(c, envelope.getLogContext()))
=======
                // todo context?
                .contextWrite(c -> contextHandler.subscriptionContext(c, envelope.getLogContext()))
>>>>>>> Stashed changes
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

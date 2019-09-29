package com.github.fred84.queue;

import static io.lettuce.core.SetArgs.Builder.ex;
import static io.lettuce.core.ZAddArgs.Builder.nx;
import static java.util.Collections.emptyMap;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.lettuce.core.KeyValue;
import io.lettuce.core.Range;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisCommandTimeoutException;
import io.lettuce.core.RedisException;
import io.lettuce.core.TransactionResult;
import io.lettuce.core.Value;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.reactive.RedisReactiveCommands;
import io.lettuce.core.api.sync.RedisCommands;
import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import javax.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

public class RedisDelayedEventService implements DelayedEventService, Closeable {

    public static final class Builder {
        private ObjectMapper mapper = new ObjectMapper();
        private RedisClient client;
        private Duration pollingTimeout = Duration.ofSeconds(1);
        private boolean enableScheduling = true;
        private Duration schedulingInterval = Duration.ofMillis(500);
        private ExecutorService threadPoolForHandlers = Executors.newFixedThreadPool(5);
        private int retryAttempts = 70;

        private Builder() {
        }

        public Builder mapper(ObjectMapper val) {
            mapper = val;
            return this;
        }

        public Builder client(RedisClient val) {
            client = val;
            return this;
        }

        public Builder pollingTimeout(Duration val) {
            pollingTimeout = val;
            return this;
        }

        public Builder enableScheduling(boolean val) {
            enableScheduling = val;
            return this;
        }

        public Builder schedulingInterval(Duration val) {
            schedulingInterval = val;
            return this;
        }

        public Builder retryAttempts(int val) {
            retryAttempts = val;
            return this;
        }

        public Builder threadPoolForHandlers(ExecutorService val) {
            threadPoolForHandlers = val;
            return this;
        }

        public RedisDelayedEventService build() {
            return new RedisDelayedEventService(this);
        }
    }

    static final String DELAYED_QUEUE = "delayed_events";
    private static final String LOCK_KEY = "delayed_events_lock";
    private static final String EVENTS_HSET = "events";
    private static final String DELIMITER = "###";
    private static Logger LOG = LoggerFactory.getLogger(RedisDelayedEventService.class);

    private final Map<Class<? extends Event>, Disposable> subscriptions = new ConcurrentHashMap<>();

    private final ObjectMapper mapper;
    private final RedisClient client;
    private final Duration pollingTimeout;
    private final int retryAttempts;
    private final Duration lockTimeout;

    private final Scheduler handlerScheduler;
    private final RedisCommands<String, String> dispatchCommands;
    private final RedisReactiveCommands<String, String> reactiveCommands;
    private final Scheduler single = Schedulers.newSingle("redis-single");
    private final ScheduledThreadPoolExecutor dispatcherExecutor = new ScheduledThreadPoolExecutor(1);

    private RedisDelayedEventService(Builder builder) {
        mapper = checkNotNull("object mapper", builder.mapper);
        client = checkNotNull("redis client", builder.client);
        pollingTimeout = checkNotShorter("polling interval", builder.pollingTimeout, Duration.ofMillis(50));
        lockTimeout = Duration.ofSeconds(2);
        retryAttempts = builder.retryAttempts;
        handlerScheduler = Schedulers.fromExecutorService(checkNotNull("handlers thread pool", builder.threadPoolForHandlers));

        this.dispatchCommands = client.connect().sync();
        this.reactiveCommands = client.connect().reactive();

        if (builder.enableScheduling) {
            dispatcherExecutor.scheduleWithFixedDelay(
                    this::dispatchDelayedMessages,
                    0,
                    checkNotShorter("scheduling interval", builder.schedulingInterval, Duration.ofMillis(50)).toNanos(),
                    TimeUnit.NANOSECONDS
            );
        }
    }

    // move to interface
    public static Builder delayedEventService() {
        return new Builder();
    }

    @Override
    public <T extends Event> boolean removeHandler(Class<T> eventType) {
        var subscription = subscriptions.remove(eventType);
        if (subscription != null) {
            subscription.dispose();
            return true;
        }
        return false;
    }

    @Override
    public <T extends Event> void addBlockingHandler(Class<T> eventType, Function<T, Boolean> handler, int parallelism) {
        addHandler(eventType, new BlockingSubscriber<>(handler), parallelism);
    }

    @Override
    public <T extends Event> void addHandler(Class<T> eventType, Function<T, Mono<Boolean>> handler, int parallelism) {
        subscriptions.computeIfAbsent(eventType, re -> {
            StatefulRedisConnection<String, String> pollingConnection = client.connect();
            var subscription = new InnerSubscriber<>(
                    handler,
                    parallelism,
                    pollingConnection,
                    handlerScheduler,
                    this::removeFromDelayedQueue
            );
            var queue = toQueueName(eventType);

            Flux
                    .generate(sink -> sink.next(0))
                    .flatMap(
                            r -> pollingConnection
                                    .reactive()
                                    .brpop(pollingTimeout.toMillis() * 1000, queue)
                                    .doOnError(e -> {
                                        if (e instanceof RedisCommandTimeoutException) {
                                            LOG.debug("polling command timed out");
                                        } else {
                                            LOG.warn("error polling redis queue", e);
                                        }
                                        pollingConnection.reset();
                                    })
                                    .onErrorReturn(KeyValue.empty("key")),
                            1, // it doesn't make sense to do requests on single connection in parallel
                            parallelism
                    )
                    .publishOn(handlerScheduler, parallelism)
                    .defaultIfEmpty(KeyValue.empty(queue))
                    .filter(Value::hasValue)
                    .map(Value::getValue)
                    .map(v -> deserialize(eventType, v))
                    .onErrorContinue((e, r) -> LOG.warn("Unable to deserialize [{}]", r, e))
                    .subscribe(subscription);

            return subscription;
        });
    }

    @Override
    public void enqueueWithDelay(Event event, Duration delay) {
        enqueueWithDelayInner(event, delay).subscribe();
    }

    @Override
    @PreDestroy
    public void close() {
        LOG.debug("shutting down delayed queue service");

        dispatcherExecutor.shutdownNow();
        subscriptions.forEach((k, v) -> v.dispose());
        single.dispose();


        dispatchCommands.getStatefulConnection().close();
        reactiveCommands.getStatefulConnection().close();

        handlerScheduler.dispose();
    }

    Mono<TransactionResult> enqueueWithDelayInner(Event event, Duration delay) {
        return executeInTransaction(() -> {
            var str = serialize(EventEnvelope.create(event, emptyMap()));
            String key = getKey(event);
            reactiveCommands.hset(EVENTS_HSET, key, str).subscribeOn(single).subscribe();
            reactiveCommands.zadd(DELAYED_QUEUE, nx(), System.currentTimeMillis() + delay.toMillis(), key).subscribeOn(single).subscribe();
        });
    }

    void dispatchDelayedMessages() {
        LOG.debug("delayed events dispatch started");

        try {
            var lock = tryLock();

            if (lock == null) {
                LOG.debug("unable to obtain lock for delayed events dispatch");
                return;
            }

            var tasksForExecution = dispatchCommands.zrangebyscore(DELAYED_QUEUE, Range.create(-1, System.currentTimeMillis()));

            if (null == tasksForExecution) {
                return;
            }

            tasksForExecution.forEach(this::handleDelayedTask);
        } catch (RedisException e) {
            LOG.warn("Error during dispatch", e);
            dispatchCommands.reset();
            throw e;
        } finally {
            unlock();
            LOG.debug("delayed events dispatch finished");
        }
    }

    private Mono<TransactionResult> removeFromDelayedQueue(Event event) {
        return executeInTransaction(() -> {
            String key = getKey(event);
            reactiveCommands.hdel(EVENTS_HSET, key).subscribeOn(single).subscribe();
            reactiveCommands.zrem(DELAYED_QUEUE, key).subscribeOn(single).subscribe();
        });
    }

    private Mono<TransactionResult> executeInTransaction(Runnable commands) {
        return Mono.defer(() -> {
            reactiveCommands.multi().subscribeOn(single).subscribe();
            commands.run();
            return reactiveCommands.exec().subscribeOn(single).doOnError(e -> reactiveCommands.getStatefulConnection().reset());
        }).subscribeOn(single);
    }

    private void handleDelayedTask(String key) {
        String rawEnvelope = dispatchCommands.hget(EVENTS_HSET, key);

        // We could have stale data because other instance already processed and deleted this key
        if (rawEnvelope == null) {
            if (dispatchCommands.zrem(DELAYED_QUEUE, key) > 0) {
                LOG.debug("key '" + key + "' not found in HSET");
            }

            return;
        }

        EventEnvelope<? extends Event> currentEnvelope = deserialize(rawEnvelope);
        EventEnvelope<? extends Event> nextEnvelope = EventEnvelope.nextAttempt(currentEnvelope);

        dispatchCommands.multi();

        if (nextEnvelope.getAttempt() < retryAttempts) {
            dispatchCommands.zadd(DELAYED_QUEUE, nextAttemptTime(nextEnvelope.getAttempt()), key);
            dispatchCommands.hset(EVENTS_HSET, key, serialize(nextEnvelope));
        } else {
            dispatchCommands.zrem(DELAYED_QUEUE, key);
            dispatchCommands.hdel(EVENTS_HSET, key);
        }
        dispatchCommands.lpush(toQueueName(currentEnvelope.getType()), serialize(currentEnvelope));

        dispatchCommands.exec();

        LOG.debug("dispatched event [{}]", currentEnvelope);
    }

    private long nextAttemptTime(int attempt) {
        return System.currentTimeMillis() + nextDelay(attempt) * 1000;
    }

    private long nextDelay(int attempt) {
        if (attempt < 10) { // first 10 attempts each 10 seconds
            return 10;
        }

        if (attempt < 10 + 10) { // next 10 attempts each minute
            return 60;
        }

        return 60 * 24; // next 50 attempts once an hour
    }

    private String serialize(EventEnvelope<? extends Event> envelope) {
        try {
            return mapper.writeValueAsString(envelope);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private EventEnvelope<?> deserialize(String rawEnvelope) {
        try {
            return mapper.readValue(rawEnvelope, EventEnvelope.class);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private <T extends Event> EventEnvelope<T> deserialize(Class<T> eventType, String rawEnvelope) {
        var envelopeType = mapper.getTypeFactory().constructParametricType(EventEnvelope.class, eventType);
        try {
            return mapper.readValue(rawEnvelope, envelopeType);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private String tryLock() {
        return dispatchCommands.set(LOCK_KEY, "value", ex(lockTimeout.toMillis() * 1000).nx());
    }

    private void unlock() {
        try {
            dispatchCommands.del(LOCK_KEY);
        } catch (RedisException e) {
            dispatchCommands.reset();
        }
    }

    static String toQueueName(Class<?> cls) {
        return cls.getSimpleName().toLowerCase();
    }

    static String getKey(Event event) {
        return event.getClass().getName() + DELIMITER + event.getId();
    }

    private static <T> T checkNotNull(String msg, T value) {
        if (value == null) {
            throw new IllegalArgumentException(String.format("'%s' should be not null", msg));
        }
        return value;
    }

    private static Duration checkNotShorter(String msg, Duration given, Duration ref) {
        if (given == null) {
            throw new IllegalArgumentException("given duration should be not null");
        }
        if (given.compareTo(ref) < 0) {
            throw new IllegalArgumentException(String.format("%s with value %s should be not shorter than %s", msg, given, ref));
        }
        return given;
    }
}
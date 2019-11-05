package com.github.fred84.queue;

import static io.lettuce.core.SetArgs.Builder.ex;
import static io.lettuce.core.ZAddArgs.Builder.nx;
import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.fred84.queue.logging.LogContext;
import com.github.fred84.queue.logging.NoopLogContext;
import com.github.fred84.queue.metrics.Metrics;
import com.github.fred84.queue.metrics.NoopMetrics;
import io.lettuce.core.KeyValue;
import io.lettuce.core.Limit;
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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;
import javax.annotation.PreDestroy;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

public class DelayedEventService implements Closeable {

    public static final class Builder {
        private ObjectMapper mapper = new ObjectMapper();
        private RedisClient client;
        private Duration pollingTimeout = Duration.ofSeconds(1);
        private boolean enableScheduling = true;
        private Duration schedulingInterval = Duration.ofMillis(500);
        private int schedulingBatchSize = 100;
        private ExecutorService threadPoolForHandlers = Executors.newFixedThreadPool(5);
        private int retryAttempts = 70;
        private Metrics metrics = new NoopMetrics();
        private LogContext logContext = new NoopLogContext();
        private String dataSetPrefix = "de_";

        private Builder() {
        }

        @NotNull
        public Builder mapper(@NotNull ObjectMapper val) {
            mapper = val;
            return this;
        }

        @NotNull
        public Builder client(@NotNull RedisClient val) {
            client = val;
            return this;
        }

        @NotNull
        public Builder pollingTimeout(@NotNull Duration val) {
            pollingTimeout = val;
            return this;
        }

        @NotNull
        Builder enableScheduling(boolean val) {
            enableScheduling = val;
            return this;
        }

        @NotNull
        public Builder schedulingInterval(@NotNull Duration val) {
            schedulingInterval = val;
            return this;
        }

        @NotNull
        public Builder retryAttempts(int val) {
            retryAttempts = val;
            return this;
        }

        @NotNull
        public Builder threadPoolForHandlers(@NotNull ExecutorService val) {
            threadPoolForHandlers = val;
            return this;
        }

        @NotNull
        public Builder metrics(@NotNull Metrics val) {
            metrics = val;
            return this;
        }

        @NotNull
        public Builder logContext(@NotNull LogContext val) {
            logContext = val;
            return this;
        }

        @NotNull
        public Builder dataSetPrefix(@NotNull String val) {
            dataSetPrefix = val;
            return this;
        }

        @NotNull
        public Builder schedulingBatchSize(int val) {
            schedulingBatchSize = val;
            return this;
        }

        @NotNull
        public DelayedEventService build() {
            return new DelayedEventService(this);
        }
    }

    private static final String DELIMITER = "###";
    private static final Logger LOG = LoggerFactory.getLogger(DelayedEventService.class);

    private final Map<Class<? extends Event>, Disposable> subscriptions = new ConcurrentHashMap<>();

    private final ObjectMapper mapper;
    private final RedisClient client;
    private final Duration pollingTimeout;
    private final int retryAttempts;
    private final Duration lockTimeout;

    private final Scheduler handlerScheduler;
    private final RedisCommands<String, String> dispatchCommands;
    private final RedisCommands<String, String> metricsCommands;
    private final RedisReactiveCommands<String, String> reactiveCommands;
    private final Scheduler single = Schedulers.newSingle("redis-single");
    private final ScheduledThreadPoolExecutor dispatcherExecutor = new ScheduledThreadPoolExecutor(1);
    private final Metrics metrics;
    private final LogContext logContext;
    private final String dataSetPrefix;
    private final Limit schedulingBatchSize;

    private final String zsetName;
    private final String lockKey;
    private final String metadataHset;

    private DelayedEventService(Builder builder) {
        mapper = requireNonNull(builder.mapper, "object mapper");
        client = requireNonNull(builder.client, "redis client");
        logContext = requireNonNull(builder.logContext, "log context");
        pollingTimeout = checkNotShorter(builder.pollingTimeout, Duration.ofMillis(50), "polling interval");
        lockTimeout = Duration.ofSeconds(2);
        retryAttempts = checkInRange(builder.retryAttempts, 1, 100, "retry attempts");
        handlerScheduler = Schedulers.fromExecutorService(requireNonNull(builder.threadPoolForHandlers, "handlers thread pool"));
        metrics = requireNonNull(builder.metrics, "metrics");
        dataSetPrefix = requireNonNull(builder.dataSetPrefix, "data set prefix");
        schedulingBatchSize = Limit.from(checkInRange(builder.schedulingBatchSize, 1, 1000, "scheduling batch size"));

        zsetName = dataSetPrefix + "delayed_events";
        lockKey = dataSetPrefix + "delayed_events_lock";
        metadataHset = dataSetPrefix + "events";

        this.dispatchCommands = client.connect().sync();
        this.metricsCommands = client.connect().sync();
        this.reactiveCommands = client.connect().reactive();

        if (builder.enableScheduling) {
            dispatcherExecutor.scheduleWithFixedDelay(
                    this::dispatchDelayedMessages,
                    0,
                    checkNotShorter(builder.schedulingInterval, Duration.ofMillis(50), "scheduling interval").toNanos(),
                    TimeUnit.NANOSECONDS
            );
        }

        metrics.registerScheduledCountSupplier(() -> metricsCommands.zcard(zsetName));
    }

    @NotNull
    public static Builder delayedEventService() {
        return new Builder();
    }

    public <T extends Event> boolean removeHandler(@NotNull Class<T> eventType) {
        requireNonNull(eventType, "event type");

        Disposable subscription = subscriptions.remove(eventType);
        if (subscription != null) {
            subscription.dispose();
            return true;
        }
        return false;
    }

    public <T extends Event> void addBlockingHandler(
            @NotNull Class<T> eventType,
            @NotNull Predicate<@NotNull T> handler,
            int parallelism
    ) {
        requireNonNull(handler, "handler");

        addHandler(eventType, new BlockingSubscriber<>(handler), parallelism);
    }

    public <T extends Event> void addHandler(
            @NotNull Class<T> eventType,
            @NotNull Function<@NotNull T, @NotNull Mono<Boolean>> handler,
            int parallelism
    ) {
        requireNonNull(eventType, "event type");
        requireNonNull(handler, "handler");
        checkInRange(parallelism, 1, 100, "parallelism");

        subscriptions.computeIfAbsent(eventType, re -> {
            StatefulRedisConnection<String, String> pollingConnection = client.connect();
            InnerSubscriber<T> subscription = new InnerSubscriber<>(
                    logContext,
                    handler,
                    parallelism,
                    pollingConnection,
                    handlerScheduler,
                    this::removeFromDelayedQueue
            );
            String queue = toQueueName(eventType);

            Flux
                    .generate(sink -> sink.next(0))
                    .flatMap(
                            r -> pollingConnection
                                    .reactive()
                                    .brpop(pollingTimeout.toNanos() * 1000, queue)
                                    .doOnError(e -> {
                                        if (e instanceof RedisCommandTimeoutException) {
                                            LOG.debug("polling command timed out");
                                        } else {
                                            LOG.warn("error polling redis queue", e);
                                        }
                                        pollingConnection.reset();
                                    })
                                    .onErrorReturn(KeyValue.empty(eventType.getName())),
                            1, // it doesn't make sense to do requests on single connection in parallel
                            parallelism
                    )
                    .publishOn(handlerScheduler, parallelism)
                    .defaultIfEmpty(KeyValue.empty(queue))
                    .filter(Value::hasValue)
                    .doOnNext(v -> metrics.incrementDequeueCounter(eventType))
                    .map(Value::getValue)
                    .map(v -> deserialize(eventType, v))
                    .onErrorContinue((e, r) -> LOG.warn("Unable to deserialize [{}]", r, e))
                    .subscribe(subscription);

            metrics.registerReadyToProcessSupplier(eventType, () -> metricsCommands.llen(toQueueName(eventType)));

            return subscription;
        });
    }

    public void enqueueWithDelay(@NotNull Event event, @NotNull Duration delay) {
        requireNonNull(event, "event");
        requireNonNull(delay, "delay");
        requireNonNull(event.getId(), "event id");

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
        metricsCommands.getStatefulConnection().close();

        handlerScheduler.dispose();
    }

    Mono<TransactionResult> enqueueWithDelayInner(Event event, Duration delay) {
        Map<String, String> context = logContext.get();

        return executeInTransaction(() -> {
            String key = getKey(event);
            String rawEnvelope = serialize(EventEnvelope.create(event, context));
            reactiveCommands.hset(metadataHset, key, rawEnvelope).subscribeOn(single).subscribe();
            reactiveCommands.zadd(zsetName, nx(), (System.currentTimeMillis() + delay.toMillis()), key).subscribeOn(single).subscribe();
        }).doOnNext(v -> metrics.incrementEnqueueCounter(event.getClass()));
    }

    void dispatchDelayedMessages() {
        LOG.debug("delayed events dispatch started");

        try {
            String lock = tryLock();

            if (lock == null) {
                LOG.debug("unable to obtain lock for delayed events dispatch");
                return;
            }

            List<String> tasksForExecution = dispatchCommands.zrangebyscore(
                    zsetName,
                    Range.create(-1, System.currentTimeMillis()),
                    schedulingBatchSize
            );

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
            reactiveCommands.hdel(metadataHset, key).subscribeOn(single).subscribe();
            reactiveCommands.zrem(zsetName, key).subscribeOn(single).subscribe();
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
        String rawEnvelope = dispatchCommands.hget(metadataHset, key);

        // We could have stale data because other instance already processed and deleted this key
        if (rawEnvelope == null) {
            if (dispatchCommands.zrem(zsetName, key) > 0) {
                LOG.debug("key '{}' not found in HSET", key);
            }

            return;
        }

        EventEnvelope<? extends Event> currentEnvelope = deserialize(Event.class, rawEnvelope);
        EventEnvelope<? extends Event> nextEnvelope = EventEnvelope.nextAttempt(currentEnvelope);

        dispatchCommands.multi();

        if (nextEnvelope.getAttempt() < retryAttempts) {
            dispatchCommands.zadd(zsetName, nextAttemptTime(nextEnvelope.getAttempt()), key);
            dispatchCommands.hset(metadataHset, key, serialize(nextEnvelope));
        } else {
            dispatchCommands.zrem(zsetName, key);
            dispatchCommands.hdel(metadataHset, key);
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
            return 10L;
        }

        if (attempt < 10 + 10) { // next 10 attempts each minute
            return 60L;
        }

        return 60L * 24; // next 50 attempts once an hour
    }

    private String serialize(EventEnvelope<? extends Event> envelope) {
        try {
            return mapper.writeValueAsString(envelope);
        } catch (JsonProcessingException e) {
            throw new UncheckedIOException(e);
        }
    }

    private <T extends Event> EventEnvelope<T> deserialize(Class<T> eventType, String rawEnvelope) {
        JavaType envelopeType = mapper.getTypeFactory().constructParametricType(EventEnvelope.class, eventType);
        try {
            return mapper.readValue(rawEnvelope, envelopeType);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private String tryLock() {
        return dispatchCommands.set(lockKey, "value", ex(lockTimeout.toMillis() * 1000).nx());
    }

    private void unlock() {
        try {
            dispatchCommands.del(lockKey);
        } catch (RedisException e) {
            dispatchCommands.reset();
        }
    }

    private String toQueueName(Class<? extends Event> cls) {
        return dataSetPrefix + cls.getSimpleName().toLowerCase();
    }

    static String getKey(Event event) {
        return event.getClass().getName() + DELIMITER + event.getId();
    }

    private static int checkInRange(int value, int min, int max, String message) {
        if (value < min || value > max) {
            throw new IllegalArgumentException(String.format("'%s' should be inside range [%s, %s]", message, min, max));
        }
        return value;
    }

    private static Duration checkNotShorter(Duration duration, Duration ref, String msg) {
        requireNonNull(duration, "given duration should be not null");

        if (duration.compareTo(ref) < 0) {
            throw new IllegalArgumentException(String.format("%s with value %s should be not shorter than %s", msg, duration, ref));
        }
        return duration;
    }
}
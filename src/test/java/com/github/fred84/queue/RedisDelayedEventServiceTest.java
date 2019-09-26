package com.github.fred84.queue;

import static com.github.fred84.queue.RedisDelayedEventService.DELAYED_QUEUE;
import static com.github.fred84.queue.RedisDelayedEventService.delayedEventService;
import static com.github.fred84.queue.RedisDelayedEventService.toQueueName;
import static java.util.Collections.emptyMap;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisCommandTimeoutException;
import io.lettuce.core.RedisConnectionException;
import io.lettuce.core.TimeoutOptions;
import io.lettuce.core.api.sync.RedisCommands;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.IntStream;
import lombok.Value;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

class RedisDelayedEventServiceTest {

    @Value
    private static class DummyEvent implements Event {
        String id;
    }

    @Value
    private static class DummyEvent2 implements Event {
        String id;
    }

    @Value
    private static class DummyEvent3 implements Event {
        String id;
    }

    private RedisClient redisClient;
    private RedisCommands<String, String> connection;
    private RedisDelayedEventService eventService;
    private final ExecutorService executor = Executors.newFixedThreadPool(10);
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final ToxiproxyClient toxiProxyClient = new ToxiproxyClient("127.0.1.1", 8474); // todo move to config
    private Proxy redisProxy;

    @BeforeEach
    void flushDb() throws IOException {
        removeOldProxies();
        redisProxy = createRedisProxy();
        redisClient = RedisClient.create("redis://localhost:63790"); // todo move to config
        redisClient.setOptions(
                ClientOptions.builder()
                        .timeoutOptions(TimeoutOptions.builder().timeoutCommands().fixedTimeout(Duration.ofMillis(500)).build())
                        .build()
        );

        eventService = delayedEventService()
                .client(redisClient)
                .mapper(objectMapper)
                .threadPoolForHandlers(executor)
                .enableScheduling(false)
                .pollingTimeout(Duration.ofSeconds(1))
                .build();

        connection = redisClient.connect().sync();
        connection.flushall();
    }

    @AfterEach
    void releaseConnection() {
        eventService.close();
        redisClient.shutdown();
    }

    boolean randomSleep(Event event) {
        try {
            TimeUnit.MILLISECONDS.sleep(1 + (new Random().nextInt(50)));
        } catch (InterruptedException e) {
            /* do nothing */
        }
        return true;
    }

    @Test
    void differentEventsHandledInParallel() throws InterruptedException {
        enqueue(90, id -> {
            var str = Integer.toString(id);
            switch (id % 3) {
                case 2:
                    return new DummyEvent3(str);
                case 1:
                    return new DummyEvent2(str);
                default:
                    return new DummyEvent(str);
            }
        });

        eventService.addBlockingHandler(DummyEvent.class, this::randomSleep, 3);
        eventService.addBlockingHandler(DummyEvent2.class, this::randomSleep, 3);
        eventService.addBlockingHandler(DummyEvent3.class, this::randomSleep, 3);

        assertThat(connection.zcard(DELAYED_QUEUE), equalTo(90L));

        eventService.dispatchDelayedMessages();

        Thread.sleep(500);

        assertThat(connection.zcard(DELAYED_QUEUE), equalTo(0L));
    }

    @Test
    void backPressureIsApplied() throws InterruptedException {
        enqueue(10);

        assertThat(connection.zcard(DELAYED_QUEUE), equalTo(10L));

        eventService.dispatchDelayedMessages();

        final Semaphore sem = new Semaphore(1);

        eventService.addBlockingHandler(
                DummyEvent.class,
                e -> {
                    sem.acquireUninterruptibly();
                    return true;
                },
                1
        );

        Thread.sleep(500);

        assertThat(connection.zcard(DELAYED_QUEUE), greaterThan(5L));
    }

    @Test
    void blockedThreadsHandling() throws InterruptedException {
        eventService.addBlockingHandler(
                DummyEvent.class,
                e -> {
                    try {
                        Thread.sleep(500);
                        return true;
                    } catch (InterruptedException ex) {
                        throw new RuntimeException(ex);
                    }
                },
                10
        );

        enqueue(20);

        eventService.dispatchDelayedMessages();

        Thread.sleep(2 * 500 + 100); // task execution takes 500, 20 should complete in 1000 (with parallism of 10), 100 ms as reserve

        assertThat(connection.zcard(DELAYED_QUEUE), equalTo(0L));
    }

    @Test
    void enqueueConcurrently() {
        Flux.merge(IntStream
                .range(0, 50)
                .parallel()
                .mapToObj(id -> eventService.enqueueWithDelayInner(new DummyEvent(Integer.toString(id)), Duration.ZERO))
                .collect(toList())
        )
                .collectList()
                .block();

        assertThat(connection.zcard(DELAYED_QUEUE), equalTo(50L));
    }

    @Test
    void handleDeserializationError() throws InterruptedException {
        eventService.addBlockingHandler(DummyEvent.class, e -> true, 1);

        enqueue(IntStream.range(0, 10));
        eventService.dispatchDelayedMessages();

        connection.lpush(toQueueName(DummyEvent.class), "[unserializable}");

        enqueue(IntStream.range(11, 20));
        eventService.dispatchDelayedMessages();

        Thread.sleep(500); // task execution takes 500, 20 should complete in 1000 (with parallism of 10), 100 ms as reserve

        assertThat(connection.zcard(DELAYED_QUEUE), equalTo(0L));
    }

    @Test
    void ableToReconnect() throws InterruptedException, IOException {
        eventService.addBlockingHandler(DummyEvent.class, e -> true, 1);

        redisProxy.delete();

        Thread.sleep(500); // more than pop timeout

        redisProxy = createRedisProxy();

        enqueue(1);
        assertThat(connection.zcard(DELAYED_QUEUE), equalTo(1L));
        eventService.dispatchDelayedMessages();

        Thread.sleep(100);

        assertThat(connection.zcard(DELAYED_QUEUE), equalTo(0L));
    }

    @Test
    void handleEmptyQueue() throws InterruptedException {
        eventService.addBlockingHandler(DummyEvent.class, e -> true, 1);

        Thread.sleep(1100);

        enqueue(1);
        assertThat(connection.zcard(DELAYED_QUEUE), equalTo(1L));
        eventService.dispatchDelayedMessages();

        Thread.sleep(100);

        assertThat(connection.zcard(DELAYED_QUEUE), equalTo(0L));
    }

    @Test
    void failToSubscribeIfConnectionNotAvailable() throws IOException {
        redisProxy.delete();

        assertThrows(RedisConnectionException.class, () -> eventService.addBlockingHandler(DummyEvent.class, e -> true, 1));
    }

    @Test
    void failToDispatchIfConnectionNotAvailable() throws IOException {
        redisProxy.delete();

        assertThrows(RedisCommandTimeoutException.class, () -> eventService.dispatchDelayedMessages());
    }

    @Test
    void subscriberErrorHandling() throws InterruptedException {
        // most possible failures covered
        eventService.addBlockingHandler(
                DummyEvent.class,
                e -> {
                    switch ((Integer.parseInt(e.getId())) % 4) {
                        case 0: throw new RuntimeException("no-no");
                        case 1: return true;
                        // case 2: return Mono.empty();
                        case 3: return null;
                        // case 4: return Mono.error(new RuntimeException("oops"));
                        default: return false;
                    }
                },
                3
        );

        enqueue(IntStream.range(0, 5));

        eventService.dispatchDelayedMessages();

        Thread.sleep(500);

        assertThat(connection.zcard(DELAYED_QUEUE), equalTo(4L));
    }

    @Test
    void dispatch() {
        DummyEvent event = new DummyEvent("99");

        eventService.enqueueWithDelayInner(event, Duration.ZERO).block();

        double score = connection.zscore(DELAYED_QUEUE, RedisDelayedEventService.getKey(event));

        eventService.dispatchDelayedMessages();

        double postDispatchScore = connection.zscore(DELAYED_QUEUE, RedisDelayedEventService.getKey(event));

        assertThat(postDispatchScore - score, greaterThan(10000.0));

        EventEnvelope<DummyEvent> restoredEvent = deserialize(connection.rpop(toQueueName(DummyEvent.class)));

        assertThat(restoredEvent, equalTo(EventEnvelope.create(event, emptyMap())));
    }

    @Test
    void duplicateItems() {
        DummyEvent event = new DummyEvent("1");

        eventService.enqueueWithDelayInner(event, Duration.ZERO).block();
        eventService.enqueueWithDelayInner(event, Duration.ZERO).block();

        assertThat(connection.zcard(DELAYED_QUEUE), equalTo(1L));
    }

    private EventEnvelope<DummyEvent> deserialize(String raw) {
        JavaType type = objectMapper.getTypeFactory().constructParametricType(EventEnvelope.class, DummyEvent.class);
        try {
            return objectMapper.readValue(raw, type);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void removeOldProxies() throws IOException {
        toxiProxyClient.getProxies().forEach(p -> {
            try {
                p.delete();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }

    private Proxy createRedisProxy() throws IOException {
        return toxiProxyClient.createProxy("redis", "localhost:63790", "localhost:6379"); // todo move to config
    }

    private void enqueue(int num) {
        enqueue(IntStream.range(0, num));
    }

    private void enqueue(int num, Function<Integer, Event> transformer) {
        enqueue(IntStream.range(0, num), transformer);
    }

    private void enqueue(IntStream stream) {
        enqueue(stream, id -> new DummyEvent(Integer.toString(id)));
    }

    private void enqueue(IntStream stream, Function<Integer, Event> transformer) {
        Flux
                .merge(stream.mapToObj(id -> eventService.enqueueWithDelayInner(transformer.apply(id), Duration.ZERO)).collect(toList()))
                .collectList()
                .block();
    }
}

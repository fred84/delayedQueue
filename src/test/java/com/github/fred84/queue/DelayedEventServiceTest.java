package com.github.fred84.queue;

import static com.github.fred84.queue.DelayedEventService.delayedEventService;
import static java.util.Collections.emptyMap;
import static java.util.Collections.synchronizedList;
import static java.util.Optional.ofNullable;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.fred84.queue.logging.MDCLogContext;
import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.Range;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisCommandTimeoutException;
import io.lettuce.core.RedisConnectionException;
import io.lettuce.core.TimeoutOptions;
import io.lettuce.core.api.sync.RedisCommands;
import java.beans.ConstructorProperties;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.function.Function;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

class DelayedEventServiceTest {

    private static class DummyEvent implements Event {

        @JsonProperty
        private final String id;

        @ConstructorProperties({"id"})
        private DummyEvent(String id) {
            this.id = id;
        }

        @Override
        public String getId() {
            return id;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof DummyEvent)) {
                return false;
            }
            DummyEvent that = (DummyEvent) o;
            return Objects.equals(id, that.id);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id);
        }
    }

    private static class DummyEvent2 implements Event {

        @JsonProperty
        private final String id;

        @ConstructorProperties({"id"})
        private DummyEvent2(String id) {
            this.id = id;
        }

        @Override
        public String getId() {
            return id;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof DummyEvent2)) {
                return false;
            }
            DummyEvent2 that = (DummyEvent2) o;
            return Objects.equals(id, that.id);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id);
        }
    }

    private static class DummyEvent3 implements Event {

        @JsonProperty
        private final String id;

        @ConstructorProperties({"id"})
        private DummyEvent3(String id) {
            this.id = id;
        }

        @Override
        public String getId() {
            return id;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof DummyEvent3)) {
                return false;
            }
            DummyEvent3 that = (DummyEvent3) o;
            return Objects.equals(id, that.id);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id);
        }
    }

    private static final String DELAYED_QUEUE = "delayed_events";
    private static final String TOXIPROXY_IP = ofNullable(System.getenv("TOXIPROXY_IP")).orElse("127.0.0.1");

    private static final Duration POLLING_TIMEOUT = Duration.ofSeconds(1);

    private RedisClient redisClient;
    private RedisCommands<String, String> connection;
    private DelayedEventService eventService;
    private final ExecutorService executor = Executors.newFixedThreadPool(10);
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final ToxiproxyClient toxiProxyClient = new ToxiproxyClient(TOXIPROXY_IP, 8474);
    private Proxy redisProxy;

    @BeforeEach
    void flushDb() throws IOException {
        removeOldProxies();
        redisProxy = createRedisProxy();
        redisClient = RedisClient.create("redis://" + TOXIPROXY_IP + ":63790");
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
                .pollingTimeout(POLLING_TIMEOUT)
                .logContext(new MDCLogContext())
                .dataSetPrefix("")
                .schedulingBatchSize(50)
                .build();

        connection = redisClient.connect().sync();
        connection.flushall();

        MDC.clear();
    }

    @AfterEach
    void releaseConnection() {
        eventService.close();
        redisClient.shutdown();
    }

    @Test
    void differentEventsHandledInParallel() throws InterruptedException {
        enqueue(30, id -> {
            String str = Integer.toString(id);
            switch (id % 3) {
                case 2:
                    return new DummyEvent3(str);
                case 1:
                    return new DummyEvent2(str);
                default:
                    return new DummyEvent(str);
            }
        });

        CountDownLatch latch = new CountDownLatch(30);

        eventService.addBlockingHandler(DummyEvent.class, e -> randomSleepBeforeCountdown(latch), 3);
        eventService.addBlockingHandler(DummyEvent2.class, e -> randomSleepBeforeCountdown(latch), 3);
        eventService.addBlockingHandler(DummyEvent3.class, e -> randomSleepBeforeCountdown(latch), 3);

        assertZsetCardinality(30L);

        eventService.dispatchDelayedMessages();

        latch.await(500, MILLISECONDS);

        waitAndAssertZsetCardinality(0L);
    }

    @Test
    void backPressureIsApplied() {
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

        waitAndAssertZsetCardinality(9);
    }

    @Test
    void verifyLogContext() throws InterruptedException {
        Map<String, String> collector = new ConcurrentHashMap<>();

        CountDownLatch latch = new CountDownLatch(3);

        eventService.addBlockingHandler(
                DummyEvent.class,
                e -> {
                    collector.put(e.getId(), MDC.get("key"));
                    latch.countDown();
                    return true;
                },
                1
        );

        enqueue(3, id -> {
            String str = Integer.toString(id);
            MDC.put("key", str);
            return new DummyEvent(str);
        });

        eventService.dispatchDelayedMessages();

        latch.await(500, MILLISECONDS);

        Map<String, String> expected = new HashMap<>();
        expected.put("0", "0");
        expected.put("1", "1");
        expected.put("2", "2");

        assertThat(collector, equalTo(expected));
    }

    @Test
    void verifyLogContextNonBlocking() throws InterruptedException {
        Map<String, String> collector = new ConcurrentHashMap<>();

        CountDownLatch latch = new CountDownLatch(3);

        eventService.addHandler(
                DummyEvent.class,
                e -> {
                    collector.put(e.getId(), MDC.get("key"));
                    latch.countDown();
                    return Mono.just(true);
                },
                1
        );

        enqueue(3, id -> {
            String str = Integer.toString(id);
            MDC.put("key", str);
            return new DummyEvent(str);
        });

        eventService.dispatchDelayedMessages();

        latch.await(500, MILLISECONDS);

        Map<String, String> expected = new HashMap<>();
        expected.put("0", "0");
        expected.put("1", "1");
        expected.put("2", "2");

        assertThat(collector, equalTo(expected));
    }

    @Test
    void blockedThreadsHandling() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(20);

        eventService.addBlockingHandler(
                DummyEvent.class,
                e -> {
                    try {
                        MILLISECONDS.sleep(500);
                        latch.countDown();
                        return true;
                    } catch (InterruptedException ex) {
                        throw new RuntimeException(ex);
                    }
                },
                10
        );

        enqueue(20);

        eventService.dispatchDelayedMessages();

        // task execution takes 500, 20 should complete in 1000 (with parallelism of 10), 100 ms as reserve
        latch.await(2 * 500 + 100, MILLISECONDS);

        waitAndAssertZsetCardinality(0L);
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

        assertZsetCardinality(50L);
    }

    @Test
    void handleDeserializationError() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(20);

        enqueue(IntStream.range(0, 10));
        eventService.dispatchDelayedMessages();

        connection.lpush(toQueueName(DummyEvent.class), "[unserializable}");

        enqueue(IntStream.range(11, 20));
        eventService.dispatchDelayedMessages();

        eventService.addBlockingHandler(
                DummyEvent.class,
                e -> {
                    latch.countDown();
                    return true;
                },
                1
        );

        latch.await(500, MILLISECONDS);

        waitAndAssertZsetCardinality(0L);
    }

    @Test
    void ableToReconnect() throws InterruptedException, IOException {
        eventService.addBlockingHandler(DummyEvent.class, e -> true, 1);

        redisProxy.delete();

        MILLISECONDS.sleep(POLLING_TIMEOUT.toMillis() + 100);

        redisProxy = createRedisProxy();

        enqueue(1);
        assertZsetCardinality(1L);

        eventService.dispatchDelayedMessages();

        waitAndAssertZsetCardinality(0L);
    }

    @Test
    void handlePollingTimeout() throws InterruptedException {
        eventService.addBlockingHandler(DummyEvent.class, e -> true, 1);

        MILLISECONDS.sleep(POLLING_TIMEOUT.toMillis() + 100);

        enqueue(1);
        assertZsetCardinality(1L);
        eventService.dispatchDelayedMessages();

        waitAndAssertZsetCardinality(0L);
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
    void blockingSubscriberErrorHandling() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(5);

        eventService.addBlockingHandler(
                DummyEvent.class,
                e -> {
                    latch.countDown();

                    switch ((Integer.parseInt(e.getId())) % 4) {
                        case 0: throw new RuntimeException("no-no");
                        case 1: return true;
                        default: return false;
                    }
                },
                3
        );

        enqueue(5);

        latch.await(500, MILLISECONDS);

        eventService.dispatchDelayedMessages();

        waitAndAssertZsetCardinality(4L);
    }

    @Test
    void subscriberErrorHandling() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(6);

        eventService.addHandler(
                DummyEvent.class,
                e -> {
                    latch.countDown();

                    switch ((Integer.parseInt(e.getId())) % 5) {
                        case 0: return Mono.just(true);
                        case 1: return Mono.error(new RuntimeException("no-no"));
                        case 2: return Mono.empty();
                        case 3: return null;
                        case 4: throw new RuntimeException("oops");
                        default: return Mono.just(false);
                    }
                },
                6
        );

        enqueue(6);

        latch.await(500, MILLISECONDS);

        eventService.dispatchDelayedMessages();

        waitAndAssertZsetCardinality(4L);
    }

    @Test
    void dispatch() {
        DummyEvent event = new DummyEvent("99");

        eventService.enqueueWithDelayInner(event, Duration.ZERO).block();

        double score = connection.zscore(DELAYED_QUEUE, DelayedEventService.getKey(event));

        eventService.dispatchDelayedMessages();

        double postDispatchScore = connection.zscore(DELAYED_QUEUE, DelayedEventService.getKey(event));

        assertThat(postDispatchScore - score, greaterThan(10000.0));

        EventEnvelope<DummyEvent> restoredEvent = deserialize(connection.rpop(toQueueName(DummyEvent.class)));

        assertThat(restoredEvent, equalTo(EventEnvelope.create(event, emptyMap())));
    }

    @Test
    void dispatchLimit() {
        enqueue(70);

        assertThat(connection.zcard(DELAYED_QUEUE), equalTo(70L));

        long maxScore = System.currentTimeMillis();

        eventService.dispatchDelayedMessages();

        assertThat(connection.zcount(DELAYED_QUEUE, Range.create(0, maxScore)), equalTo(20L));
    }

    @Test
    void duplicateItems() {
        DummyEvent event = new DummyEvent("1");

        eventService.enqueueWithDelayInner(event, Duration.ZERO).block();
        eventService.enqueueWithDelayInner(event, Duration.ZERO).block();

        assertZsetCardinality(1L);
    }

    @Test
    void removeHandler() {
        enqueue(10);

        eventService.addBlockingHandler(DummyEvent.class, e -> true, 1);

        eventService.dispatchDelayedMessages();
        waitAndAssertZsetCardinality(0L);

        assertThat(eventService.removeHandler(DummyEvent.class), equalTo(true));
        assertThat(eventService.removeHandler(DummyEvent.class), equalTo(false));

        enqueue(10);

        waitAndAssertZsetCardinality(10L);
    }

    @Test
    void failedEventsDuringRefreshWouldBeHandledLater() throws InterruptedException {
        enqueue(10);

        CountDownLatch latch1 = new CountDownLatch(1);
        CountDownLatch latch2 = new CountDownLatch(1);
        List<String> handledIds = synchronizedList(new ArrayList<>());


        eventService.addBlockingHandler(
                DummyEvent.class,
                e -> {
                    handledIds.add(e.getId());
                    latch1.countDown();
                    try {
                        latch2.await();
                    } catch (InterruptedException ex) {
                        throw new RuntimeException(ex);
                    }
                    return true;
                },
                1
        );

        eventService.dispatchDelayedMessages();

        assertThat(latch1.await(500, MILLISECONDS), equalTo(true));

        eventService.refreshSubscriptions();
        latch2.countDown();

        sleepMillis(500);

        waitAndAssertZsetCardinality(10 - handledIds.size());

        Set<String> unhandledIds = IntStream.range(0, 10)
                .mapToObj(Integer::toString)
                .filter(i -> !handledIds.contains(i))
                .collect(toSet());


        Set<String> leftForProcessing = connection
                .hkeys("events")
                .stream()
                .filter(s -> s.contains("###")).map(s -> s.split("###")[1])
                .collect(toSet());

        assertThat(leftForProcessing, equalTo(unhandledIds));
    }

    @Test
    void closeClientsAfterRefresh() {
        int initNumber = serviceConnectionsCount();

        eventService.addBlockingHandler(DummyEvent.class, e -> true, 1);

        sleepMillis(100);

        assertThat(serviceConnectionsCount() - initNumber, is(1));
        eventService.refreshSubscriptions();
        eventService.refreshSubscriptions();
        eventService.refreshSubscriptions();

        sleepMillis(100);

        assertThat(serviceConnectionsCount() - initNumber, is(1));
    }

    @Test
    void enqueueNulls() {
        assertThrows(NullPointerException.class, () -> eventService.enqueueWithDelay(new DummyEvent("1"), null));
        assertThrows(NullPointerException.class, () -> eventService.enqueueWithDelay(null, Duration.ZERO));
        assertThrows(NullPointerException.class, () -> eventService.enqueueWithDelay(new DummyEvent(null), Duration.ZERO));
    }

    @Test
    void validateAddHandler() {
        assertThrows(NullPointerException.class, () -> eventService.addHandler(null, e -> Mono.just(true), 1));
        assertThrows(NullPointerException.class, () -> eventService.addHandler(DummyEvent.class, null, 1));
        assertThrows(IllegalArgumentException.class, () -> eventService.addHandler(DummyEvent.class, e -> Mono.just(true), 0));
        assertThrows(IllegalArgumentException.class, () -> eventService.addHandler(DummyEvent.class, e -> Mono.just(true), 101));
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
        return toxiProxyClient.createProxy("redis", TOXIPROXY_IP + ":63790", "localhost:6379");
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

    private String toQueueName(Class<? extends Event> cls) {
        return cls.getSimpleName().toLowerCase();
    }

    private boolean randomSleepBeforeCountdown(CountDownLatch latch) {
        sleepMillis(1 + new Random().nextInt(30));
        latch.countDown();
        return true;
    }

    private void waitAndAssertZsetCardinality(long expected) {
        for (int i = 0; i < 5; i++) {
            sleepMillis(25);

            if (connection.zcard(DELAYED_QUEUE) == expected) {
                return;
            }
        }

        assertZsetCardinality(expected);
    }

    private void assertZsetCardinality(long expected) {
        assertThat(connection.zcard(DELAYED_QUEUE), equalTo(expected));
    }

    private void sleepMillis(long duration) {
        try {
            MILLISECONDS.sleep(duration);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private int serviceConnectionsCount() {
        return connection.clientList().split("\\r?\\n").length;
    }
}
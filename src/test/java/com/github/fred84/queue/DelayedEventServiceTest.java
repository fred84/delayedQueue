package com.github.fred84.queue;

import static com.github.fred84.queue.DelayedEventService.delayedEventService;
import static java.util.Optional.ofNullable;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.fred84.queue.metrics.NoopMetrics;
import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.Range;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisException;
import io.lettuce.core.RedisURI;
import io.lettuce.core.TimeoutOptions;
import io.lettuce.core.api.sync.RedisCommands;
import java.beans.ConstructorProperties;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.function.Function;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

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
    private static final Function<DummyEvent, Mono<Boolean>> DUMMY_HANDLER = e -> Mono.just(true);
    private static final int SCHEDULING_BATCH_SIZE = 50;

    private RedisClient redisClient;
    private RedisCommands<String, String> connection;
    private DelayedEventService eventService;
    private final ExecutorService executor = Executors.newFixedThreadPool(10);
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final ToxiproxyClient toxiProxyClient = new ToxiproxyClient(TOXIPROXY_IP, 8474);
    private Proxy redisProxy;

    @BeforeEach
    void setUp() throws IOException {
        removeOldProxies();
        redisProxy = createRedisProxy();

        redisClient = RedisClient.create(new RedisURI(TOXIPROXY_IP, 63790, Duration.ofSeconds(10)));
        redisClient.setOptions(
                ClientOptions.builder()
                        .timeoutOptions(TimeoutOptions.builder().timeoutCommands().fixedTimeout(Duration.ofMillis(500)).build())
                        .build()
        );

        eventService = delayedEventService()
                .client(redisClient)
                .mapper(objectMapper)
                .handlerScheduler(Schedulers.fromExecutorService(executor))
                .schedulingInterval(Duration.ofSeconds(1))
                .schedulingBatchSize(SCHEDULING_BATCH_SIZE)
                .enableScheduling(false)
                .pollingTimeout(POLLING_TIMEOUT)
                .dataSetPrefix("")
                .retryAttempts(10)
                .metrics(new NoopMetrics())
                .refreshSubscriptionsInterval(Duration.ofMinutes(5))
                .build();

        connection = redisClient.connect().sync();
        connection.flushall();
    }

    @AfterEach
    void releaseConnection() {
        eventService.close();
        redisClient.shutdown();
    }

    @Test
    void shouldHandleDifferentEventsInParallel() throws InterruptedException {
        // given
        int parallelism = 3;
        int total = 30;
        int maxDelayMs = 30;
        CountDownLatch latch = new CountDownLatch(total);

        eventService.postDeleteInterceptor = e -> latch.countDown();

        eventService.addHandler(DummyEvent.class, e -> sleepAndTrue(randomMillis(maxDelayMs)), parallelism);
        eventService.addHandler(DummyEvent2.class, e -> sleepAndTrue(randomMillis(maxDelayMs)), parallelism);
        eventService.addHandler(DummyEvent3.class, e -> sleepAndTrue(randomMillis(maxDelayMs)), parallelism);
        // and events are queued
        enqueue(total, id -> {
            String str = Integer.toString(id);
            switch (id % 3) {
                case 2:
                    return new DummyEvent3(str);
                case 1:
                    return new DummyEvent2(str);
                default:
                    return new DummyEvent(str);
            }
        }).block();

        assertScheduledMessagesInZsetCount(total);
        // when
        eventService.dispatchDelayedMessages();
        // then
        assertLatchCompleted(latch, total / parallelism * maxDelayMs);
        assertScheduledMessagesInZsetCount(0);
    }

    @Test
    void shouldAdherePrefetchLimit() {
        // given
        int total = 10;
        int prefetch = 1;
        Semaphore sem = new Semaphore(1);

        eventService.addHandler(
                DummyEvent.class,
                e -> Mono.just(true).doOnNext(t -> sem.acquireUninterruptibly()),
                prefetch
        );
        // and events are queued
        enqueue(total);
        assertScheduledMessagesInZsetCount(total);
        // when
        eventService.dispatchDelayedMessages();
        // then
        assertScheduledMessagesInZsetCount(total - prefetch);
    }

    @Test
    void shouldCompleteInTimelyMannerForLongRunningHandlers() throws InterruptedException {
        // given
        final int total = 20;
        final Duration timeout = Duration.ofMillis(500);
        final int parallelism = 10;
        CountDownLatch latch = new CountDownLatch(20);

        eventService.postDeleteInterceptor = e -> latch.countDown();

        eventService.addHandler(
                DummyEvent.class,
                e -> Mono.delay(timeout).thenReturn(true),
                parallelism
        );
        // and events are enqueued
        enqueue(total);
        assertScheduledMessagesInZsetCount(total);

        // when
        eventService.dispatchDelayedMessages();
        // then task execution takes 500, 20 should complete in 1000 (with parallelism of 10), 100 ms as reserve
        assertThat(latch.await(total / parallelism * timeout.toMillis() + 100, MILLISECONDS)).isTrue();

        assertScheduledMessagesInZsetCount(0);
    }

    @Test
    void shouldBeAbleToEnqueueConcurrently() {
        // when
        Flux.merge(IntStream
                .range(0, 50)
                .parallel()
                .mapToObj(id -> eventService.enqueueWithDelayNonBlocking(new DummyEvent(Integer.toString(id)), Duration.ZERO))
                .collect(toList())
        ).then().block();
        // then
        assertScheduledMessagesInZsetCount(50);
    }

    @Test
    void shouldHandleDeserializationError() {
        // given
        int total = 20;
        // and events are enqueued
        enqueue(total);
        assertScheduledMessagesInZsetCount(total);
        // and a malformed event is placed in the beginning of a list
        connection.lpush(toQueueName(DummyEvent.class), "[unserializable}");
        eventService.dispatchDelayedMessages();
        // and all events are moved to list
        assertThat(connection.llen(toQueueName(DummyEvent.class))).isEqualTo(total + 1);
        // when
        CountDownLatch latch = new CountDownLatch(total);

        eventService.postDeleteInterceptor = e -> latch.countDown();

        eventService.addHandler(
                DummyEvent.class,
                e -> Mono.just(true),
                1
        );
        // then
        assertLatchCompleted(latch, 100);
        assertScheduledMessagesInZsetCount(0);
    }

    @Test
    void shouldBeAbleToReconnect() throws InterruptedException, IOException {
        // given
        CountDownLatch latch = new CountDownLatch(1);

        eventService.postDeleteInterceptor = e -> latch.countDown();

        eventService.addHandler(DummyEvent.class, DUMMY_HANDLER, 1);
        // when connection is broken
        redisProxy.delete();
        MILLISECONDS.sleep(POLLING_TIMEOUT.toMillis() + 100);
        // and connection is restored
        createRedisProxy();
        // then new event is handled
        enqueue(1);
        assertScheduledMessagesInZsetCount(1L);
        eventService.dispatchDelayedMessages();

        assertLatchCompleted(latch, 100);
        assertScheduledMessagesInZsetCount(0);
    }

    @Test
    void shouldHandlePollingTimeout() throws InterruptedException {
        // given
        CountDownLatch latch = new CountDownLatch(1);

        eventService.postDeleteInterceptor = e -> latch.countDown();

        eventService.addHandler(DummyEvent.class, DUMMY_HANDLER, 1);
        // and no events have arrived during a polling interval
        MILLISECONDS.sleep(POLLING_TIMEOUT.toMillis() + 100);
        // and and event is queued
        enqueue(1);
        assertScheduledMessagesInZsetCount(1L);
        // then
        eventService.dispatchDelayedMessages();

        // then
        assertLatchCompleted(latch, 100);
        assertScheduledMessagesInZsetCount(0);
    }

    @Test
    void shouldFailToDispatchIfConnectionNotAvailable() throws IOException {
        redisProxy.delete();
        redisClient.setOptions(ClientOptions.builder().autoReconnect(false).build());

        assertThrows(RedisException.class, () -> eventService.dispatchDelayedMessages());
    }

    @Test
    void shouldHandleSubscriberErrors() {
        // given an erroneous handler
        int total = 6;
        CountDownLatch preLatch = new CountDownLatch(total);

        CountDownLatch postLatch = new CountDownLatch(1);
        eventService.postDeleteInterceptor = e -> postLatch.countDown();

        eventService.addHandler(
                DummyEvent.class,
                e -> {
                    preLatch.countDown();

                    switch ((Integer.parseInt(e.getId())) % total) {
                        // valid
                        case 0:
                            return Mono.just(true);
                        // invalid
                        case 1:
                            return Mono.error(new RuntimeException("mono error"));
                        case 2:
                            return Mono.empty();
                        case 3:
                            return null;
                        case 4:
                            throw new RuntimeException("raw error");
                        default:
                            return Mono.just(false);
                    }
                },
                1
        );
        // and events are enqueued
        enqueue(total);

        assertScheduledMessagesInZsetCount(total);
        // when
        eventService.dispatchDelayedMessages();
        // then only valid events are handled

        assertLatchCompleted(preLatch, 100);
        assertLatchCompleted(postLatch, 100);

        assertScheduledMessagesInZsetCount(total - 1);
    }

    @Test
    void shouldRescheduleEventForLaterTimeDuringDispatch() {
        // given event is queued
        DummyEvent event = new DummyEvent("99");
        eventService.enqueueWithDelayNonBlocking(event, Duration.ZERO).block();

        double initialScore = connection.zscore(DELAYED_QUEUE, DelayedEventService.getKey(event));
        // when
        eventService.dispatchDelayedMessages();
        double postDispatchScore = connection.zscore(DELAYED_QUEUE, DelayedEventService.getKey(event));
        // then the post dispatch score is 10 sec ahead
        assertThat(postDispatchScore - initialScore).isGreaterThan(10000.0);
    }

    @Test
    void shouldAdhereDispatchLimit() {
        // given
        int extra = 20;
        int total = SCHEDULING_BATCH_SIZE + extra;
        // and events are scheduled
        enqueue(total);
        assertScheduledMessagesInZsetCount(total);
        long maxScore = System.currentTimeMillis();
        // when
        eventService.dispatchDelayedMessages();
        // then only SCHEDULING_BATCH_SIZE are rescheduled
        assertThat(connection.zcount(DELAYED_QUEUE, Range.create(0, maxScore))).isEqualTo(extra);
    }

    @Test
    void shouldNotDuplicateSameEvents() {
        // given
        DummyEvent event = new DummyEvent("1");
        // when
        eventService.enqueueWithDelayNonBlocking(event, Duration.ZERO).block();
        eventService.enqueueWithDelayNonBlocking(event, Duration.ZERO).block();
        // then
        assertScheduledMessagesInZsetCount(1L);
    }

    @Test
    void shouldBeAbleToRemoveHandler() {
        // given
        CountDownLatch latch = new CountDownLatch(1);
        eventService.postDeleteInterceptor = e -> latch.countDown();

        eventService.addHandler(DummyEvent.class, DUMMY_HANDLER, 1);
        // and events are enqueued
        enqueue(10);
        eventService.dispatchDelayedMessages();

        assertLatchCompleted(latch, 100);
        assertScheduledMessagesInZsetCount(0L);
        // when
        assertThat(eventService.removeHandler(DummyEvent.class)).isTrue();
        assertThat(eventService.removeHandler(DummyEvent.class)).isFalse();
        // then new events are not handled
        enqueue(10);
        assertScheduledMessagesInZsetCount(10L);
    }

    @Test
    void shouldReleaseOldConnectionOnSubscriptionRefresh() {
        // given
        int initNumber = serviceConnectionsCount();
        // when
        eventService.addHandler(DummyEvent.class, DUMMY_HANDLER, 1);
        sleepMillis(100);
        assertThat(serviceConnectionsCount() - initNumber).isEqualTo(1);
        // and subscription is refreshed
        eventService.refreshSubscriptions();
        sleepMillis(100);
        // then
        assertThat(serviceConnectionsCount() - initNumber).isEqualTo(1);
    }

    @Test
    void shouldNotAllowToEnqueueNulls() {
        assertThrows(NullPointerException.class, () -> eventService.enqueueWithDelayNonBlocking(new DummyEvent("1"), null));
        assertThrows(NullPointerException.class, () -> eventService.enqueueWithDelayNonBlocking(null, Duration.ZERO));
        assertThrows(NullPointerException.class, () -> eventService.enqueueWithDelayNonBlocking(new DummyEvent(null), Duration.ZERO));
    }

    @Test
    void shouldValidateAddedHandler() {
        assertThrows(NullPointerException.class, () -> eventService.addHandler(null, DUMMY_HANDLER, 1));
        assertThrows(NullPointerException.class, () -> eventService.addHandler(DummyEvent.class, null, 1));
        assertThrows(IllegalArgumentException.class, () -> eventService.addHandler(DummyEvent.class, DUMMY_HANDLER, 0));
        assertThrows(IllegalArgumentException.class, () -> eventService.addHandler(DummyEvent.class, DUMMY_HANDLER, 101));
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
        return toxiProxyClient.createProxy("redis", "0.0.0.0:63790", "dq_redis:6379");
    }

    private void enqueue(int num) {
        enqueue(IntStream.range(0, num)).block();
    }

    private Mono<Void> enqueue(int num, Function<Integer, Event> transformer) {
        return enqueue(IntStream.range(0, num), transformer);
    }

    private Mono<Void> enqueue(IntStream stream) {
        return enqueue(stream, id -> new DummyEvent(Integer.toString(id)));
    }

    private Mono<Void> enqueue(IntStream stream, Function<Integer, Event> transformer) {
        return Flux
                .fromStream(stream::boxed)
                .flatMap(id -> eventService.enqueueWithDelayNonBlocking(transformer.apply(id), Duration.ZERO))
                .then();
    }

    private String toQueueName(Class<? extends Event> cls) {
        return cls.getSimpleName().toLowerCase();
    }

    private Mono<Boolean> sleepAndTrue(Duration duration) {
        return Mono.delay(duration).thenReturn(true);
    }

    private Duration randomMillis(int bound) {
        return Duration.ofMillis(new Random().nextInt(bound));
    }

    private void assertScheduledMessagesInZsetCount(long expected) {
        assertThat(connection.zcard(DELAYED_QUEUE))
                .as("delayed messages count in zset '%s'", DELAYED_QUEUE)
                .isEqualTo(expected);
    }

    private void assertLatchCompleted(CountDownLatch latch, long afterMs) {
        try {
            assertThat(latch.await(afterMs + 100, MILLISECONDS))
                    .as("Latch should be completed within %d (+100 reserve) ms; remaining count %d", afterMs, latch.getCount())
                    .isTrue();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
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
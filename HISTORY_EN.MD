Several years ago we had to solve how to enqueue events with an arbitrary delay, e.g. check a status of a payment 3 hours later,
or send notification to a client in 45 minutes.
At that point of time, we didn't find suitable libraries to accomplish this task, which didn't require us to spend time on configuration and
maintenance. After analyzing possible solutions we ended up building our own small library
[delayed queue](https://github.com/fred84/delayedQueue) in Java language on top of `Redis` storage engine.
In this article I'll explain capabilities of this library, alternatives and problems we solved during creation process. 

## Functionality

So what exactly is `delayed queue` capable of? An event, added to delayed queue, is delivered to a handler after arbitrary delay.
If event handling is unsuccessful, it would be delivered again later. However, the number of retries is limited.
`Redis` does not provide any resilient guarantees, thus users should be prepared to deal with this.
Regardless, in clustered configuration `Redis` shows sufficiently high reliability and we haven't faced any issues during 1.5 years of usage.

### API

#### Add an event to a queue
```java
eventService.enqueueWithDelayNonBlocking(new DummyEvent("1"), Duration.ofHours(1)).subscribe();
```

Please beware, that the method returns `Mono`, so you have to call one of the methods below to launch execution:
- `subscribe(...)`
- `block()`

More details on this could be found in the documentation of `Project Reactor`. Event context could be added in the following manner:

```java
eventService.enqueueWithDelayNonBlocking(new DummyEvent("1"), Duration.ofHours(1), Map.of("key", "value")).subscribe();
```

#### Register an event handler
```java
eventService.addHandler(DummyEvent.class, e -> Mono.just(true), 1);
```

the same action, but with event context:

```java
eventService.addHandler(
        DummyEvent.class,
        e -> Mono
            .subscriberContext()
            .doOnNext(ctx -> {
                Map<String, String> eventContext = ctx.get("eventContext");
                log.info("context key {}", eventContext.get("key"));
            })
            .thenReturn(true),
        1
);
```

#### Remove event handler
```java
eventService.removeHandler(DummyEvent.class);
```

#### Create service
 
You can rely on defaults:

```java
import static com.github.fred84.queue.DelayedEventService.delayedEventService;

var eventService = delayedEventService().client(redisClient).build();
```

or configure everything by yourself:

```java
import static com.github.fred84.queue.DelayedEventService.delayedEventService;

var eventService = delayedEventService()
        .client(redisClient)
        .mapper(objectMapper)
        .handlerScheduler(Schedulers.fromExecutorService(executor))
        .schedulingInterval(Duration.ofSeconds(1))
        .schedulingBatchSize(SCHEDULING_BATCH_SIZE)
        .enableScheduling(false)
        .pollingTimeout(POLLING_TIMEOUT)
        .eventContextHandler(new DefaultEventContextHandler())
        .dataSetPrefix("")
        .retryAttempts(10)
        .metrics(new NoopMetrics())
        .refreshSubscriptionsInterval(Duration.ofMinutes(5))
        .build();
```

Shutting down the service (and all open connections to `Redis`) could be done via `eventService.close()` or framework with support
for the lifecycle annotation `@javax.annotation.PreDestroy`.

#### Metrics

Any system is prone to face faults and we have to monitor it. For `delayed queue` we should look out for:
- `Redis` overall memory usage
- size of `list` for every event type ("delayed.queue.ready.for.handling.count" + a tag with event type)

## History

Here is a brief overview on how `delayed queue` evolved over time. In 2018, we launched our small project
in [Amazon Web Services](https://aws.amazon.com/). Only 2 engineers were in charge of this project, so adding more 
components, which required configuration and maintenance was discouraged. The main aim was "to use components managed by AWS
unless they were too pricey".

### Possible candidates

- [RabbitMQ](https://www.rabbitmq.com)
- JMS
- [AWS SQS](https://aws.amazon.com/sqs/)

The first two were rejected due to maintenance requirements. The last one (SQS) was not considered as the maximum
delay could not be bigger than 15 minutes.

### Overlooked candidates

Unfortunately, we missed some libraries, which could have solved our needs and were discovered much-much later:
- [Netflix dyno-queues](https://github.com/Netflix/dyno-queues)
- [AWS MQ](https://aws.amazon.com/amazon-mq/)

The first one uses the same technology stack (Java and Redis), the latter is built on top of [ActiveMQ](https://activemq.apache.org/).

### First naive implementation

Initially, we already had a backup mechanism with polling a relational database once a day. After reading several articles on
organizing simple delayed queues, we decided to build our solution around `Redis`, not RDBMS. The structure inside `Redis`
is as following:
- an event is added to [sorted sets](https://redis.io/topics/data-types), where `weight` serves as future execution time
- once `weight` becomes lower that `now`, the event is moved from `sorted_set` to `list` (which could be used as a `queue` with `push` and `pop` methods)

First version of the dispatcher, responsible for moving events from `sorted set` to `list` was:
(simplified code is shown here and after):

```java
var events = redis.zrangebyscore("delayed_events", Range.create(-1, System.currentTimeMillis()), 100);
events.forEach(key -> {
  var payload = extractPayload(key);
  var listName = extractType(key);
  redis.lpush(listName, payload);
  redis.zrem("delayed_events", key);
});
```

Event handlers were built on top of [Spring Integration](https://spring.io/projects/spring-integration),
which executed the following command under the hood:

```java
redis.brpop(listName)
```

The first problems came soon.

### Unreliable dispatcher

If an error appeared in the process of adding an element to the `list` (e.g. a connection timeout after the element was added),
the dispatcher retried to do this operation, which resulted in multiple copies of an event being added to the `list`.
Luckily, `Redis` supports transactions, so we wrapped the 2 commands above into transaction.

```java
events.forEach(key -> {
  ...
  redis.multi();
  redis.zrem("delayed_events", key);
  redis.lpush(listName, payload);
  redis.exec();
});
```

### Unreliable handler

On the other side of the `list` lurked another problem. If a handler failed, an event would be lost forever.
As a solution, we chose to reschedule an event to a later period of time (unless a maximum number of attempts had been reached)
and delete it only after successful processing by the handler.

```java
events.forEach(key -> {
  ...
  redis.multi();
  redis.zadd("delayed_events", nextAttempt(key))
  redis.zrem("delayed_events", key);
  redis.lpush(listName, payload);
  redis.exec();
});
```

### Non-unique event

As I mentioned before, we already had the fallback mechanism, which polled RDBMS and re-added all "pending" entities to 
`delayed queue`. At that time, `key` in `sorted set` was structured as ```metadata;payload```, with a mutable metadata (
e.g. an attempt number, a log context, ...) and an immutable payload. So again, it resulted in multiple copies of an event 
being added to the `list`. To solve this problem, we moved ```metadata;payload``` to new structure `Redis hset` and kept only 
`event type + event id` as a key in `sorted set`. Consequently, event enqueueing transformed from:

```java
var envelope = metadata + SEPARATOR + payload;
redis.zadd(envelope, scheduledAt);
```

into

```java
var envelope = metadata + SEPARATOR + payload;
var key = eventType + SEPARATOR + eventId;

redis.multi();
redis.zadd(key, scheduledAt);
redis.hset("metadata", key, envelope)
redis.exec();
```

### Sequential dispatcher launch

All our handlers were idempotent, so we didn't pay much attention to event duplicates. However, there was still room for
improvement. The dispatchers were running on all our application instances and from time to time were launched simultaneously.
This again resulted in duplicate events in `list`. The solution was the trivial lock with small TTL:

```java
redis.set(lockKey, "value", ex(lockTimeout.toMillis() * 1000).nx());
```

## Fork into independent project

When the necessity for using `delayed queue` in a project without `Spring` emerged, we moved it to a standalone project.
To accomplish it, we were forced to remove the following dependencies:

- [Spring Data Redis](https://spring.io/projects/spring-data-redis)
- [Spring integration](https://spring.io/projects/spring-integration)

The first one was easily replaced with the `Lettuce` Redis driver. The second led to much more changes.
At that point of time I had already acquired some experience working with reactive streams in general and with `Project Reactor`
in particular. So we chose "hot stream" as a source for our handlers.
To achieve the uniform distribution of events among handlers on different application instances we had to implement our own
 [Subscriber](https://www.reactive-streams.org/reactive-streams-1.0.0-javadoc/org/reactivestreams/Subscriber.html):

```java
redis
  .reactive()
  .brpop(timeout, queue)
  .map(e -> deserialize(e))
  .subscribe(new InnerSubscriber<>(handler, ... params ..))
```

and

```java
class InnerSubscriber<T extends Event> extends BaseSubscriber<EventEnvelope<T>> {

    @Override
    protected void hookOnNext(@NotNull EventEnvelope<T> envelope) {
        Mono<Boolean> promise = handler.apply(envelope.getPayload());
        promise.subscribe(r -> request(1));
    }
}
```

As a result, we created a library, which delivers events to registered handlers (unlike `Netflix dyno queue`, where you
have to poll a storage for events).

### What's next?

- add Kotlin DSL. Currently, our new projects are created in Kotlin language, so it would be handy to use `suspend fun` instead of
 direct interaction with `Project Reactor` API
- add configurable intervals for retries
- replace `Redis transactions` with `LUA script`

## Links

- [delayed queue](https://github.com/fred84/delayedQueue)
- [Redis sorted sets](https://redis.io/topics/data-types)
- [Project reactor](https://projectreactor.io)
- [Jackson](https://github.com/FasterXML/jackson)
- [Micrometer](https://micrometer.io)
- [Lettuce](https://lettuce.io)
- [Netflix dyno-queues](https://github.com/Netflix/dyno-queues)
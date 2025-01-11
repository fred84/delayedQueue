# Delayed queue

[![Build Status](https://travis-ci.com/fred84/delayedQueue.svg?branch=master)](https://travis-ci.com/fred84/delayedQueue)

`DelayedQueue` is a library to enqueue and later handle immutable events. It is written in `java` on top of [reactor](https://github.com/reactor) and [lettuce](https://github.com/lettuce-io/lettuce-core).
[Redis](https://redis.io) is the only available storage engine. Queue doesn't provide durability guarantees but in pair with clusterized `redis` installation 
it is suitable for many use cases.

Key features are:
- event handling is retriable at increased intervals between attempts
- support for the `@PreDestroy` lifecycle annotation
- sending metrics using [Micrometer](https://github.com/micrometer-metrics/micrometer)

`DelayedQueue` are highly opinionated (hence customizable), with very little configuration needed to start using it. 
If you want more control consider using Netflix's [dyno-queue](https://github.com/Netflix/dyno-queues).

## Installation

Minimal supported java version is 1.8.

```groovy
repositories {
    maven { url 'https://jitpack.io' }
}
```

```groovy
dependencies {
    implementation 'com.github.fred84:delayedQueue:0.5.0'
}
```

## Examples

### Minimal configuration

```java
import static com.github.fred84.queue.DelayedEventService.delayedEventService;

var eventService = delayedEventService().client(redisClient).build();
```

### Full configuration

```java
import static com.github.fred84.queue.DelayedEventService.delayedEventService;

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
```

### Add event handler

```java
eventService.addHandler(DummyEvent.class, e -> Mono.just(true), 1);
```

### Enqueue event

```java
eventService.enqueueWithDelayNonBlocking(new DummyEvent("1"), Duration.ofHours(1)).subscribe();
```

### Close service

```java
eventService.close();
```

## Contribution

Contributions are welcome. Just create a pull request.
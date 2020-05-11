# Delayed queue

[![Build Status](https://travis-ci.com/fred84/delayedQueue.svg?branch=master)](https://travis-ci.com/fred84/delayedQueue)

`DelayedQueue` is a library to enqueue and later handle immutable events. It is written in `java` on top of [reactor](https://github.com/reactor) and [lettuce](https://github.com/lettuce-io/lettuce-core).
[Redis](https://redis.io) is the only available storage engine. Queue doesn't provide durability guarantees but in pair with clusterized `redis` installation 
it is suitable for many use cases.

Key features are:
- event handling is retriable at increased intervals between attempts
- logging context could be restored right before event handling
- support for `lifecycle` annotations
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
    implementation 'com.github.fred84:delayedQueue:0.3.5'
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

var eventService = delayedEventService()
        .client(redisClient)
        .mapper(objectMapper)
        .threadPoolForHandlers(executor)
        .logContext(new MDCLogContext())
        .metrics(new MicrometerMetrics(registry))
        .build();
```

### Add event handler

```java
eventService.addHandler(DummyEvent.class, e -> Mono.just(true), 1);
```
### Add blocking event handler

```java
eventService.addBlockingHandler(DummyEvent.class, e -> true, 1);
```

### Enqueue event

```java
eventService.enqueueWithDelayNonBlocking(new DummyEvent("1"), Duration.ofHours(1)).subscribe(...);
```

### Close service

```java
eventService.close();
```

### Event context

```java
eventService.addHandler(DummyEvent.class, e -> Mono.subscriberContext().doOnNext(ctx -> { ... }).thenReturn(true), 1);

eventService.enqueueWithDelayNonBlocking(new DummyEvent("1"), Duration.ofHours(1), Map.of("a", "b")).subscribe(...);
```

## Changelog

### 0.3.5

- support for reactor subscriber context added

### 0.3.4

- the `enqueueWithDelay` method is now blocking
- the `enqueueWithDelayNonBlocking` method added as non-blocking counterpart

### 0.3.3

- fixes for MDC logging context

### 0.3.2

- fix undisposed old subscriptions after a refresh

### 0.3

- a periodic refresh for subscriptions

## Contribution

Contributions are welcome. Just create a pull request.
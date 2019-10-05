# Delayed queue

[![Build Status](https://travis-ci.com/fred84/delayedQueue.svg?branch=master)](https://travis-ci.com/fred84/delayedQueue)

`DelayedQueue` is a library to enqueue and later handle immutable events. It is written in `java` on top of `reactor` and `lettuce`.
`Redis` is the only available storage engine. Queue doesn't provide durability guarantees but in pair with clusterized `redis` installation 
it is suitable for many use cases.

Key features are:
- event handling is retriable at increased intervals between attempts
- logging context could be restored right before event handling
- support for `lifecycle` annotations
- sending metrics using `Micrometer`

`DelayedQueue` are highly opinionated (hence customizable), with very little configuration needed to start using it. 
If you want more control consider using Netflix's [dyno-queue](https://github.com/Netflix/dyno-queues).

## Installation

Minimal supported java version is 11.

```groovy
        repositories {
			...
			maven { url 'https://jitpack.io' }
		}
```

```groovy
    dependencies {
	        implementation 'com.github.fred84:delayedQueue:0.2'
	}
```

gradle + link to jitpack

## Examples

### Minimal configuration

```java
var eventService = DelayedEventService.delayedEventService().client(redisClient).build();
```

### Full configuration

```java
var eventService = DelayedEventService.delayedEventService()
                .client(redisClient)
                .mapper(objectMapper)
                .threadPoolForHandlers(executor)
                .enableScheduling(false)
                .pollingTimeout(Duration.ofSeconds(1))
                .logContext(new MDCLogContext())
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
eventService.enqueueWithDelay(new DummyEvent("1"), Duration.ofHours(1));
```

## Contribution

Contributions are welcome. Just create pull request.
## Changelog

### 0.4

Backward incompatible change

- the deprecated `enqueueWithDelay` was removed
- support for `BlockingHandler` was removed
- MDC-backed `LogContext` replaced with subscription context based `EventContextHandler`
- `DelayedEventService` is now accepts a [Scheduler](https://projectreactor.io/docs/core/release/api/reactor/core/scheduler/Scheduler.html) instead of an [ExecutorService](https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/ExecutorService.html)

### 0.3.4

- the `enqueueWithDelay` method is now blocking and marked as deprecated
- the `enqueueWithDelayNonBlocking` is now added as non-blocking alternative

### 0.3.3

- fix bugs with MDC logging context

### 0.3.2

- fix the bug with old subscriptions are not being disposed after refresh

### 0.3

- support for a periodic refresh of subscriptions
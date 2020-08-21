Пару лет назад в одном из проектов мы столкнулись с необходимостью откладывать выполнение некоего действия на определенный
промежуток времени. Например, узнать статус платежа через три часа или повторно отправить уведомление через 45 минут. 
Однако на тот момент мы не нашли подходящих библиотек, способных "откладывать" и не требующих дополнительного времени на настройку
и эксплуатацию. Мы проанализировали возможные варианты и написали собственную маленькую библиотеку
[delayed queue](https://github.com/fred84/delayedQueue) на Java с использованием Redis в роли хранилища. 
В этой статье я расскажу про возможности библиотеки, ее альтернативы и те "грабли", на которые мы наткнулись в процессе. 

## Функционал

Итак, что же делает delayed queue? Событие, добавленное в отложенную очередь, доставляется обработчику через указанный промежуток времени. Если процесс обработки завершается неудачно, событие будет доставлено снова позднее. При этом максимальное количество попыток ограничено. 
Redis не дает гарантий сохранности, и к потере событий **нужно** быть готовым. Однако в кластерном варианте Redis показывает достаточно высокую надежность, и мы ни разу не столкнулись с этим за полтора года эксплуатации. 

### API

#### Добавить событие в очередь
```java
eventService.enqueueWithDelayNonBlocking(new DummyEvent("1"), Duration.ofHours(1)).subscribe();
```

Обратите внимание, что метод возвращает `Mono`, поэтому для запуска надо выполнить одно из следующих действия:
- `subscribe(...)`
- `block()`

Более подробные разъяснения приводятся в документации по `Project Reactor`. Контекст добавляется к событию следующим образом:

```java
eventService.enqueueWithDelayNonBlocking(new DummyEvent("1"), Duration.ofHours(1), Map.of("key", "value")).subscribe();
```

#### Зарегистрировать обработчик событий
```java
eventService.addHandler(DummyEvent.class, e -> Mono.just(true), 1);
```

если вместе с событием необходимо обработать пришедший контекст, то:

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

#### Удалить обработчик событий
```java
eventService.removeHandler(DummyEvent.class);
```

#### Создание сервиса
 
Можно воспользоваться настройками "по-умолчанию":

```java
import static com.github.fred84.queue.DelayedEventService.delayedEventService;

var eventService = delayedEventService().client(redisClient).build();
```

или сконфигурировать всё самому:

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

Завершить работу сервиса (и всех открытых им соединений в Redis) можно `eventService.close()` или через фреймворк, поддерживающий аннотацию `@javax.annotation.PreDestroy`.

#### Метрики

С любой системой что-то может пойти не так, за ней надо приглядывать. В первую очередь вас должны интересовать:
- общий размер памяти, используемый Redis;
- количество событий, готовых к обработке (метрика "delayed.queue.ready.for.handling.count" и тэгом конкретного типа события)



## История

Теперь в двух словах о том, как появился и развивался delayed queue. В 2018 году
наш маленький проект был запущен в [Amazon Web Services](https://aws.amazon.com/). Он разрабатывался и поддерживался двумя инженерами, и добавлять в него требующие обслуживания компоненты было накладно с точки зрения времени обслуживания системы.  Основным правилом было: "используй подходящие компоненты, обслуживаемые Amazon-ом, если это не стоит очень дорого".

### Готовые кандидаты

Мы рассматривали: 

- [RabbitMQ](https://www.rabbitmq.com)
- JMS
- [AWS SQS](https://aws.amazon.com/sqs/)

Первые два были отсеяны из-за необходимости их настраивать и обслуживать, к тому же с JMS не доставало опыта работы. SQS исключили, поскольку максимальное время задержки не превышает 15 минут.

### Первая наивная реализация

На момент старта у нас уже был резервный вариант с обходом "зависших сущностей" в реляционной БД раз в сутки. Почитав статьи про организацию очередей отложенных событий, мы выбрали Redis со следующей структурой:
- событие добавляется в [sorted sets](https://redis.io/topics/data-types), где весом выступает время ее будущего выполнения
- по наступлению времени выполнения событие перекладывается из "sorted_set" в "list" (может использоваться в режиме очереди)

Забегая вперед, на тот момент уже полгода существовал проект [Netflix dyno-queues](https://github.com/Netflix/dyno-queues)
с примерно похожим принципом работы. Однако тогдя я его, к сожалению, еще не нашел.

Первая версия диспетчера, который перекладывал "созревшие события" из sorted set в list, выглядела примерно так (здесь и далее приведен упрощенный код):

```java
var events = redis.zrangebyscore("delayed_events", Range.create(-1, System.currentTimeMillis()), 100);
events.forEach(key -> {
  var payload = extractPayload(key);
  var listName = extractType(key);
  redis.lpush(listName, payload);
  redis.zrem("delayed_events", key);
});
```

Обработчики событий были сделаны поверх [Spring Integration](https://spring.io/projects/spring-integration),
который в свою очередь фактически делал:

```java
redis.brpop(listName)
```

Первые проблемы не заставили себя долго ждать.

### Ненадежный диспетчер

При возникновении ошибки при добавлении в "list" (например, отвалилось соединение), событие помещалось в list несколько раз. Поскольку Redis поддерживает транзакции, мы просто обернули эти 2 метода.

```java
events.forEach(key -> {
  ...
  redis.multi();
  redis.zrem("delayed_events", key);
  redis.lpush(listName, payload);
  redis.exec();
});
```

### Ненадежный обработчик

С другой стороны list-a нас поджидала еще одна проблема. Событие пропадало навсегда, если ошибка происходила внутри обработчика. 
Решением стала замена удаления элемента из "sorted_set" на перезапись его на более позднее время и удаление только после успешного завершения обработки.

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

### Не уникальное событие

Как я уже упоминал, у нас изначально был запасной механизм, который обходил "зависшие сущности" в БД и добавлял в "delayed queue"
еще одно. Внутри "sorted set" ключ выглядел как
```metadata;payload```, где payload у нас неизменный, а вот metadata у следующей попытки для одного и того-же события отличалась. 
В итоге мы могли получить дубликат и много ненужных повторных попыток обработки. Эту ситуацию мы решили, вынеся изменяемую metadata и неизменный payload в `Redis hset` и оставив в "sorted set" только тип и идентификатор события. В итоге регистрация события превратилась из 

```java
var envelope = metadata + SEPARATOR + payload;
redis.zadd(envelope, scheduledAt);
```
в 
```java
var envelope = metadata + SEPARATOR + payload;
var key = eventType + SEPARATOR + eventId;

redis.multi();
redis.zadd(key, scheduledAt);
redis.hset("metadata", key, envelope)
redis.exec();
```

### Последовательный запуск диспетчера

Все наши обработчики были идемпотентными, и мы не беспокоились о дубликатах событий. Тем не менее, на нескольких экземплярах приложения диспечеры иногда запускались одновременно и добавляли в list одно и то же событие. Добавление банальной блокировки с коротким TTL сделало код чуть более эффективным: 

```java
redis.set(lockKey, "value", ex(lockTimeout.toMillis() * 1000).nx());
```

## Эволюция в отдельный проект

Когда такую же задачу понадобилось решить в проекте без Spring, функционал был вынесен в самостоятельную библиотеку. 
"Под нож" пошли зависимости:

- [Spring Data Redis](https://spring.io/projects/spring-data-redis)
- [Spring integration](https://spring.io/projects/spring-integration)

Первая была легко заменена на использование Lettuce напрямую, а со второй все оказалось чуть сложнее. К этому моменту у меня был небольшой опыт работы с реактивными стримами в общем и с Project Reactor в частности, поэтому источником событий для обработчика стал "горячий стрим".
Чтобы добиться равномерного распределения событий между обработчиками в разных экземплярах приложения, пришлось реалиховать свой собственный [Subscriber](https://www.reactive-streams.org/reactive-streams-1.0.0-javadoc/org/reactivestreams/Subscriber.html) 

```java
redis
  .reactive()
  .brpop(timeout, queue)
  .map(e -> deserialize(e))
  .subscribe(new InnerSubscriber<>(handler, ... params ..))
```

и

```java
class InnerSubscriber<T extends Event> extends BaseSubscriber<EventEnvelope<T>> {

    @Override
    protected void hookOnNext(@NotNull EventEnvelope<T> envelope) {
        Mono<Boolean> promise = handler.apply(envelope.getPayload());
        promise.subscribe(r -> request(1));
    }
}
```

В итоге мы получили библиотеку, которая сама доставляет события в зарегистрированные обработчики (в отличии от Netflix dyno queue, гда надо самому poll-ить события).

### Что планируем дальше?

- добавить Kotlin DSL. Новые проекты я все чаще начинаю на Kotlin и использовать `suspend fun` вместо API `Project Reactor` будет удобнее
- сделать настраиваемыми интервалы между повторными попытками

## Ccылки

- [delayed queue](https://github.com/fred84/delayedQueue)
- [Redis sorted sets](https://redis.io/topics/data-types)
- [Project reactor](https://projectreactor.io)
- [Jackson](https://github.com/FasterXML/jackson)
- [Micrometer](https://micrometer.io)
- [Lettuce](https://lettuce.io)
- [Netflix dyno-queues](https://github.com/Netflix/dyno-queues)
В одном из проектов мне нужно было с увеличивающейся периодичностью выполнять какое-либо действие.
Например узнавать статус платежа или повторно отправить непринятое другой системой уведомление.
После анализа доступных решений мы взяли и написали свою маленькую библиотеку 
[delayed queue](https://github.com/fred84/delayedQueue) на Java с использованием Redis в роли хранилища. 
В этой статье я расскажу: 
- про возможности библиотеки
- альтернативы
- историю развития (или скорее список граблей, на которые мы наступили)

## Функционал

Событие, добавленное в отложенную очередь будет доставлено обработчику через указанный интервал времени.
Если в процессе обработки события произойдет ошибка или обработчик сообщит, что требуется еще одна попытка обработки,
то событие будет доставлено обработчику еще раз чуть позже (но не более максимального количества попыток).

Redis не дает гарантий сохранности,к потере событий **нужно** быть готовым,
но в кластерном варианте Redis показывает достаточно высокую надежность.
Мы за 1.5 года эксплуатации с этим ни разу не столкнулись.

### API

#### Добавить событие в очередь
```java
eventService.enqueueWithDelayNonBlocking(new DummyEvent("1"), Duration.ofHours(1)).subscribe();
```

Обратите внимание, что метод возвращает `Mono`, поэтому чтобы оно запустилось, надо выполнить одно из следующих действия:
- `subscribe(...)`
- `block()`

За более подробными разъяснениями можно сходить в документацию по `Project Reactor`. Если нужно добавить к событию контекст,
то это делается следующим образом:

```java
eventService.enqueueWithDelayNonBlocking(new DummyEvent("1"), Duration.ofHours(1), Map.of("key", "value")).subscribe();
```

#### Зарегистрировать обработчик событий
```java
eventService.addHandler(DummyEvent.class, e -> Mono.just(true), 1);
```

если вместе с событием мы хотим обработать пришедший контекст:

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

Завершить сервис (и все открытые им соединения в Redis) можно `eventService.close()` или отдать это на откуп фреймворку,
поддерживающему аннотацию `@javax.annotation.PreDestroy`.

#### Метрики

С любой системой что-то может пойти не так, за ней надо приглядывать. В первую очередь вас должны интересовать:
- общий размер памяти, используемый Redis;
- количество событий, готовых к обработке (метрика "delayed.queue.ready.for.handling.count" и тэгом конкретного типа события)



## История

С общим описанием сервиса на этом все, теперь можно вспомнить как он развивался. На дворе был 2018 год,
наш маленький проект был запущен в [Amazon Web Services](https://aws.amazon.com/) и разрабатывался 
и поддерживался всего 2 инженерами, так что добавлять в него требующие обслуживания компоненты мы было накладно с точки зрения
времени обслуживания системы.  Основным правилом было "используй подходящие компоненты, обслуживаемые Amazon-ом, 
если это не стоит очень дорого".

### Готовые кандидаты

Были рассмотрены
- [RabbitMQ](https://www.rabbitmq.com)
- JMS
- [AWS SQS](https://aws.amazon.com/sqs/)

Первые 2 кандидата были отсеяны по причине необходимости их настраивать и обслуживать. А с JMS еще и опыта работы не доставало.
Следующим мы исключили SQS, так как максимальное время задержки всего 15 минут.

### Первая наивная реализация

На момент старта у нас уже был резервный вариант с обходом "зависших сущностей" в реляционной БД раз в сутки. Почитав статей по организации
очередей отложенных событий, мы выбрали Redis со следующей структурой на следующей структуре:
- событие добавляется в [sorted sets](https://redis.io/topics/data-types), где весом выступает время ее будущего выполнения
- по наступлению времени выполнения событие перекладывается из "sorted_set" в "list" (может использоваться в режиме очереди)

Забегая вперед, на тот момент уже как полгода существовал проект [Netflix dyno-queues](https://github.com/Netflix/dyno-queues)
с примерно похожим принципом работы, но я, к сожалению, его тогда не нашел.

Первая версия диспечера, который перекладывал "созревшие события" из sorted set в list выглядел примерно так
(здесь и далее приведен упрощенный код):

```java
var events = redis.zrangebyscore("delayed_events", Range.create(-1, System.currentTimeMillis()), 100);
events.forEach(key -> {
  var payload = extractPayload(key);
  var listName = extractType(key);
  redis.lpush(listName, payload);
  redis.zrem("delayed_events", key);
});
```

Обработчики событий были сделано с использованием сделаны поверх [Spring Integration](https://spring.io/projects/spring-integration),
которые в свою очередь фактически делал:

```java
redis.brpop(listName)
```

Первые проблемы не заставили себя долго ждать.

### Ненадежный диспетчер

Если при добавлении в "list" произойдет ошибка (например отвалилось соединение), то событие будет положено в list несколько раз
Redis поддерживает транзакции и мы просто обернули эти 2 метода.

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

С друго когда list-a нас поджидала еще одна проблема. Если внутри обратчика происходила ошибка, то событие пропадало навсегда. 
Решение стало вместо удаления элемента из "sorted_set" переписывать его на более позднее время и удалять только после успешного
завершения обработчика.

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

Как я уже упоминал, у нас изначально был запасной механизм, который обходил зависшие сущности в БД и добавлял в "delayed queue"
еще одно событие. Внутри "sorted set" ключ выглядел как
```metadata;payload```, где payload у нас неизменный, а вот metadata у следующей попытки для одного и того-же события отличалась. 
В итоге мы могли получить дубликат и много ненужных повторных попыток обработки. Эту ситуацию мы решили,
вынеся и изменяемую metadata и неизменный payload в `Redis hset`. В итоге регистрация события превратилась из 

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

Все наши обработчики были идемпотентными и мы не беспокоились о дубликатах событий. Тем не менее на нескольких экземплярах
приложения диспечеры иногда запускались одновременно и добавляли в list одно и то же событие. Добавление банальной блокировки
с коротким TTL сделало код чуть более эффективным: 

```java
redis.set(lockKey, "value", ex(lockTimeout.toMillis() * 1000).nx());
```

## Эволюция в отдельный проект

Когда отложенная очередь понадобилась в другом проекте без Spring, функционал был вынесен в самостоятельную библиотеку. 
Под нож пошли зависимости:
- [Spring Data Redis](https://spring.io/projects/spring-data-redis)
- [Spring integration](https://spring.io/projects/spring-integration)

Первый был легко заменен на использование Lettuce напрямую. В вот со вторым все было чуть сложнее. К этому моменту у меня был
небольшой опыт работы с реактивными стримами в общем и с Project Reactor в частности. Целью была запрашивать следующий элемент
из Redis только после того, как был обработан предыдущий элемент. Это было сделано с помощью своей реализации
[Subscriber](https://www.reactive-streams.org/reactive-streams-1.0.0-javadoc/org/reactivestreams/Subscriber.html) 

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

На этом шаге я завершаю рассказ про эволюцию "delayed queue". 

### Планы на будущее

- добавить Kotlin DSL. Новые проекты я все чаще начинаю на Kotlin и использовать `suspend fun` вместо API `Project Reactor` будет удобнее
- настройка интервалов между повторными попытками


## Заключение

В итоге мы получили библиотеку, которая сама доставляет события в зарегистрированные обработчики (в отличии от 
Netflix dyno queue, гда надо самому poll-ить события).

## Ccылки

- [delayed queue](https://github.com/fred84/delayedQueue)
- [Redis sorted sets](https://redis.io/topics/data-types)
- [Project reactor](https://projectreactor.io)
- [Jackson](https://github.com/FasterXML/jackson)
- [Micrometer](https://micrometer.io)
- [Lettuce](https://lettuce.io)
- [Netflix dyno-queues](https://github.com/Netflix/dyno-queues)

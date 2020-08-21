В одном из проектов мне нужно было с увеличивающейся периодичностью выполнять какое-либо действие.
Например узнавать статус платежа или повторно отправить непринятое другой системой уведомление.
После анализа доступных решений мы взяли и написали свою маленькую библиотеку 
[delayed queue](https://github.com/fred84/delayedQueue) на Java с использованием Redis в роли хранилища. 
В этой статье я расскажу: 
- про ее возможности библиотеки
- альтернативы
- и историю развития (или скорее список граблей, на которые мы наступили)

## Функционал

У нас есть обработчик определенного типа событий, мы можем сохранить событие с отложенным сроком исполнения и когда оно созреет,
это событие будет доставлено обработчику. Если в процессе обработки будет выброшено исключение,
то событие будет доставлено еще раз чуть позже (но не более максимального количества попыток). Redis не дает гарантий сохранности,
к потере событий **нужно** быть готовым, но в кластерном варианте Redis показывает достаточно высокую надежность.
Мы за 1.5 года эксплуатации с этим ни разу не столкнулись.

### API

#### зарегистрировать обработчик события
```java
eventService.addHandler(DummyEvent.class, e -> Mono.just(true), 1);
```

если вместе с событие может прийти дополнительный контекст и он нам интересен:

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

#### удалить обработчик события
```java
eventService.removeHandler(DummyEvent.class);
```

#### добавить событие в очередь
```java
eventService.enqueueWithDelayNonBlocking(new DummyEvent("id"), Duration.ofHours(1)).subscribe();
```
и тоже самое с контекстом:

```java
eventService.enqueueWithDelayNonBlocking(new DummyEvent("1"), Duration.ofHours(1), Map.of("key", "value")).subscribe();
```

####Создание сервиса
 
Можно положиться на настройки "по-умолчанию":

```java
import static com.github.fred84.queue.DelayedEventService.delayedEventService;

var eventService = delayedEventService().client(redisClient).build();
```

или можно сконфигурировать самому:

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

Потушить сервис (и все открытые им соединения в Redis) можно `eventService.close()` или зарегистрировать сервис во фреймворке,
поддерживающий `@javax.annotation.PreDestroy`.

С любой системой что-то может пойти не так, за ней надо отслеживать. В первую очередь будущих пользователей должны интересовать
- общий размер памяти, используемый Redis;
- количество событий, готовых к обработке (метрика "delayed.queue.ready.for.handling.count" и тэгом конкретного типа события)

Как я уже упомянул, "delayed queue" написана на Java и тянет за собой только [Lettuce](https://lettuce.io), 
[Jackson](https://github.com/FasterXML/jackson), [Micrometer](https://micrometer.io) и [Project reactor](https://projectreactor.io).
Минимальная поддерживаемая версия Java - 8.

## История

На дворе был 2018 год, наш маленький проект был запущен в [Amazon Web Services](https://aws.amazon.com/) и разрабатывался 
и поддерживался всего 2 инженерами, так что добавлять в него требующие обслуживания компоненты мы посчитали нецелесообразным. 
Основным правилом было "используй подходящие компоненты, обслуживаемые Amazon-ом, если это не стоит очень дорого".

### Готовые варианты варианты

Вначале были рассмотрены
- [RabbitMQ](https://www.rabbitmq.com)
- JMS

Оба брокера сообщений были отсеяны по причине необходимости их обслуживать. А со вторым еще и опыта работы не доставало.
Следующим мы исключили [AWS SQS](https://aws.amazon.com/sqs/), так как максимальное время задержки было всего 15 минут.

Выбирая между реляционный БД и Redis мы решили использовать последнего, по следующим причинам:
- у нас уже был резервный вариант с обходом "зависших сущностей" в реляционной БД раз в сутки
- в редисе если удобная операция blocking poll, не надо постоянно ходить и спрашивать "есть ли что-то новое"

Забегая вперед, на тот момент уже как полгода существовал проект [Netflix dyno-queues](https://github.com/Netflix/dyno-queues)
с примерно похожим принципом работы, но я, к сожалению, его тогда не нашел.

### Первая наивная реализация

Почитав статей по организации очередей отложенных событий, мы остановились на следующей структуре:
- событие добавляется в [sorted sets](https://redis.io/topics/data-types), где весом выступает время ее будущего выполнения
- по наступлению времени выполнения событие перекладывается из "sorted_set" в "list" (может использоваться в режиме очереди)

Упрощенный dispatcher для первого шага выглядел 

```java
var events = redis.zrangebyscore("delayed_events", Range.create(-1, System.currentTimeMillis()), 100);
events.forEach(key -> {
  var payload = extractPayload(key);
  var listName = extractType(key);
  redis.zrem("delayed_events", key);
  redis.lpush(listName, payload);
});
```

Обработчики вначале изначально были сделано с использованием сделаны через [Spring Integration](https://spring.io/projects/spring-integration),
которые под капотом фактически делал:

```
redis..brpop(listName)
```

Первые проблемы не заставили себя долго ждать

### Ненадежный диспетчер
Можно заметить, что в наивной реализации диспетчера операции добавления в "list" и удаления из "sorted_set" выполняются последовательно без обработки ошибок. 
Если при добавлении в "list" произойдет ошибка (например отвалилось соединение), то событие безвозвратно утеряно.
Благо redis поддерживает транзакции и мы стали их использовать

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

Ровно такая же проблема подстерегала нас с другой стороны list-а. Если обработчик во время исполнения падал, то событие пропадало навсегда. 
Было решено при диспетчеризации вместо удаления элемента из "sorted_set" переписывать его на более позднее время и удалять только после успешного
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

Как я уже упоминал, у нас изначально был запасной механизм, который обходил какую-нибудь табличку в БД и для объектов,
которые долго висят в не конечном статусе и повторно добавлял в "delayed queue" событие. 
Так как события записывались у нас в "sorted set" в виде
```metadata;payload```, где payload у нас неизменный, а вот metadata у следующей попытки для одного и того-же события отличалась. 
В итоге мы могли получить дубликат и много ненужных повторных попыток обработки. Эту ситуацию мы решили,
вынеся и изменяемую metadata и неизменный payload в hset (ссылка). В итоге в ключе регистрация события из 

```java
var envelope = metadata + SEPARATOR + payload;
redis.zadd(envelope, scheduledAt);
```

превратилась в 

```java
var envelope = metadata + SEPARATOR + payload;
var key = eventType + SEPARATOR + eventId;

redis.multi();
redis.zadd(key, scheduledAt);
redis.hset("metadata", key, envelope)
redis.exec();
```

### Параллельный запуск диспетчера

Изначально мы писали все наши обработчики идемпотентными и не беспокоились о том, что событие может быть доставлено несколько раз. 
Такое могло случиться, если диспетчеры на разных экземплярах приложения запустились почти одновременно. Это было исправлено 
банальным локом с коротким TTL в том же Redis 

```java
redis.set(lockKey, "value", ex(lockTimeout.toMillis() * 1000).nx());
```

## Эволюция в отдельный проект

Когда отложенная очередь понадобилась в другом проекте без Spring, функционал был вынесен в самостоятельную библиотеку. Под нож пошли зависимости от
- [Spring Data Redis](https://spring.io/projects/spring-data-redis)
- [Spring integration](https://spring.io/projects/spring-integration)

Первый был легко заменен на использование Lettuce напрямую. В вот со вторым все было чуть сложнее. К этому моменту у меня был
небольшой опыт работы с реактивными стримами в Project Reactor и 

```java
redis
  .reactive()
  .brpop(timeout, queue)
  .map(e -> deserialize(e))
  .subscribe(new InnerSubscriber<>(handler, ... params ..))
```

Цель была запрашивать следующий элемент только после того, как был обработан предыдущий. Это было реализовано с помощью
своей реализации [Subscriber](https://www.reactive-streams.org/reactive-streams-1.0.0-javadoc/org/reactivestreams/Subscriber.html) 

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

- добавить Kotlin DSL. Новые проекты я все чаще начинаю на Kotlin и использовать `suspend fun` вместо API `Project Reactor` было бы сподручнее
- конфигурование интервалов между последующими попытками для каждого типа событий


## Заключение
На выходе мы получили библиотеку в push-стиле (в отличии от pull в netflix) где достаточно зарегистрировать подписчика 
на свое событие и больше ни о чем не думать (ну кроме мониторинга). 
В процессе написания и ранней эксплуатации мы исправили несколько глупых граблей и неожиданно узнали, 
что уже есть библиотека со схожим функционалом.

## Ccылки

- [delayed queue](https://github.com/fred84/delayedQueue)
- [Redis sorted sets](https://redis.io/topics/data-types)
- [Project reactor](https://projectreactor.io)
- [Jackson](https://github.com/FasterXML/jackson)
- [Micrometer](https://micrometer.io)
- [Lettuce](https://lettuce.io)
- [Netflix dyno-queues](https://github.com/Netflix/dyno-queues)

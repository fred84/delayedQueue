package com.github.fred84.queue;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import java.util.Map;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.Value;

@Value
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
class EventEnvelope<T extends Event> {

    @JsonTypeInfo(
            use = JsonTypeInfo.Id.CLASS,
            include = JsonTypeInfo.As.EXTERNAL_PROPERTY,
            property = "type"
    )
    private final T payload;
    private final int attempt;
    private final Map<String, String> logContext;

    static <R extends Event> EventEnvelope<R> create(R payload, Map<String, String> logContext) {
        return new EventEnvelope<>(payload, 1, logContext);
    }

    static <R extends Event> EventEnvelope<R> nextAttempt(EventEnvelope<R> current) {
        return new EventEnvelope<>(current.payload, current.attempt + 1, current.logContext);
    }

    @SuppressWarnings("unchecked")
    Class<T> getType() {
        return (Class<T>)payload.getClass();
    }

    @Override
    public String toString() {
        return String.format("redis event %s#%s with attempt %s", payload.getClass().getName(), payload.getId(), attempt);
    }
}
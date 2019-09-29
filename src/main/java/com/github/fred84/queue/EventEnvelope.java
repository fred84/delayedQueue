package com.github.fred84.queue;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import java.beans.ConstructorProperties;
import java.util.Map;
import java.util.Objects;

final class EventEnvelope<T extends Event> {

    @JsonTypeInfo(
            use = JsonTypeInfo.Id.CLASS,
            include = JsonTypeInfo.As.EXTERNAL_PROPERTY,
            property = "type"
    )
    private final T payload;
    @JsonProperty
    private final int attempt;
    @JsonProperty
    private final Map<String, String> logContext;

    @ConstructorProperties({"payload", "attempt", "logContext"})
    private EventEnvelope(T payload, int attempt, Map<String, String> logContext) {
        this.payload = payload;
        this.attempt = attempt;
        this.logContext = logContext;
    }

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

    T getPayload() {
        return this.payload;
    }

    int getAttempt() {
        return this.attempt;
    }

    Map<String, String> getLogContext() {
        return this.logContext;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (!(o instanceof EventEnvelope)) {
            return false;
        } else {
            EventEnvelope<?> that = (EventEnvelope)o;
            return this.attempt == that.attempt
                    && Objects.equals(this.payload, that.payload)
                    && Objects.equals(this.logContext, that.logContext);
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.payload, this.attempt, this.logContext);
    }

    @Override
    public String toString() {
        return String.format("redis event %s#%s with attempt %s", payload.getClass().getName(), payload.getId(), attempt);
    }
}
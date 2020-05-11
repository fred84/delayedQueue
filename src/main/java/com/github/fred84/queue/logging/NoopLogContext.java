package com.github.fred84.queue.logging;

import static java.util.Collections.emptyMap;

import java.util.Map;
import reactor.util.context.Context;

public final class NoopLogContext implements LogContext {

    @Override
    public Map<String, String> getDefault() {
        return emptyMap();
    }

    @Override
    public void applyToMDC(Map<String, String> context) {
        // no-op
    }

    @Override
    public Context applyToSubscriberContext(Context subscriberContext, Map<String, String> eventContext) {
        return subscriberContext;
    }
}

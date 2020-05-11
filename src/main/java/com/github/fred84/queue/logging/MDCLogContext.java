package com.github.fred84.queue.logging;

import static java.util.Collections.emptyMap;

import java.util.Map;
import org.slf4j.MDC;
import reactor.util.context.Context;

public final class MDCLogContext implements LogContext {

    @Override
    public Map<String, String> getDefault() {
        Map<String, String> context = MDC.getCopyOfContextMap();
        return context != null ? context : emptyMap();
    }

    @Override
    public void applyToMDC(Map<String, String> context) {
        if (context != null) {
            MDC.setContextMap(context);
        }
    }

    @Override
    public Context applyToSubscriberContext(Context subscriberContext, Map<String, String> eventContext) {
        return subscriberContext.put("eventContext", eventContext);
    }
}

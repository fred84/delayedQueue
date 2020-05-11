package com.github.fred84.queue.logging;

import java.util.Map;
import reactor.util.context.Context;

/**
 * Event context handler (formerly responsible only for MDC / log related functionality)
 */
public interface LogContext {

    /**
     * Supplier of an event context in case a context
     * not provided explicitly during enqueue
     */
    Map<String, String> getDefault();

    void applyToMDC(Map<String, String> eventContext);

    Context applyToSubscriberContext(Context subscriberContext, Map<String, String> eventContext);
}

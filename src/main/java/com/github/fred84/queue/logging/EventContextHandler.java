package com.github.fred84.queue.logging;

import java.util.Map;
import reactor.util.context.Context;

public interface EventContextHandler {

    /**
     * Extract an event context from a subscription context at the enqueue stage
     */
    Map<String, String> eventContext(Context subscriptionContext);

    /**
     * Add an event context to a subscription context before passing an event to a handler
     */
    Context subscriptionContext(Context originalSubscriptionContext, Map<String, String> eventContext);

    void applyToMDC(Context subscriptionContext);
}

package com.github.fred84.queue.logging;

import static java.util.Collections.emptyMap;

import java.util.Map;
import org.slf4j.MDC;
import reactor.util.context.Context;

public final class DefaultEventContextHandler implements EventContextHandler {

    private static String KEY = "eventContext";

    @Override
    public Map<String, String> eventContext(Context subscriptionContext) {
        return subscriptionContext.getOrDefault(KEY, emptyMap());
    }

    @Override
    public Context subscriptionContext(Context originalSubscriptionContext, Map<String, String> eventContext) {
        return originalSubscriptionContext.put(KEY, eventContext);
    }

    @Override
    public void applyToMDC(Context subscriptionContext) {
        eventContext(subscriptionContext).forEach(MDC::put);
    }
}

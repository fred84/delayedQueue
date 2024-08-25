package com.github.fred84.queue.context;

import static java.util.Collections.emptyMap;

import java.util.Map;
import reactor.util.context.Context;
import reactor.util.context.ContextView;

public final class DefaultEventContextHandler implements EventContextHandler {

    private static final String KEY = "eventContext";

    @Override
    public Map<String, String> eventContext(ContextView subscriptionContext) {
        return subscriptionContext.getOrDefault(KEY, emptyMap());
    }

    @Override
    public Context subscriptionContext(Context originalSubscriptionContext, Map<String, String> eventContext) {
        return originalSubscriptionContext.put(KEY, eventContext);
    }
}

package com.github.fred84.queue.logging;

import static java.util.Collections.emptyMap;

import java.util.Map;
import reactor.util.context.Context;

public final class NoopEventContextHandler implements EventContextHandler {

    @Override
    public Map<String, String> eventContext(Context subscriptionContext) {
        return emptyMap();
    }

    @Override
    public Context subscriptionContext(Context originalSubscriptionContext, Map<String, String> eventContext) {
        return originalSubscriptionContext;
    }
}

package com.github.fred84.queue.logging;

import static java.util.Collections.emptyMap;

import java.util.Map;

public class NoopLogContext implements LogContext {

    @Override
    public Map<String, String> get() {
        return emptyMap();
    }

    @Override
    public void set(Map<String, String> context) {
        // no-op
    }
}

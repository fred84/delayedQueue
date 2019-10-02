package com.github.fred84.queue.logging;

import static java.util.Collections.emptyMap;

import java.util.Map;

public class NoopLogContextHelper implements LogContextHelper {


    @Override
    public Map<String, String> getLogContext() {
        return emptyMap();
    }

    @Override
    public void setLogContext(Map<String, String> context) {
        // do nothing
    }
}

package com.github.fred84.queue.logging;

import static java.util.Collections.emptyMap;

import java.util.Map;
import org.slf4j.MDC;

public class MDCLogContextHelper implements LogContextHelper {

    @Override
    public Map<String, String> getLogContext() {
        Map<String, String> context = MDC.getCopyOfContextMap();
        return context != null ? context : emptyMap();
    }

    @Override
    public void setLogContext(Map<String, String> context) {
        if (context != null && !context.isEmpty()) {
            MDC.setContextMap(context);
        }
    }
}

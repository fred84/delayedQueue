package com.github.fred84.queue.logging;

import java.util.Map;

public interface LogContext {

    Map<String, String> get();

    void set(Map<String, String> context);
}

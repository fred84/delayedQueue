package com.github.fred84.queue.logging;

import java.util.Map;

public interface LogContextHelper {

    Map<String, String> getLogContext();

    void setLogContext(Map<String, String> context);
}

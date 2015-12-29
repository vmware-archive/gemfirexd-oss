/*
 * Copyright (c) 2010-2015 Pivotal Software, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
package com.gemstone.gemfire.internal.util;

import java.util.logging.Handler;
import java.util.logging.Logger;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.GemFireLevel;
import com.gemstone.gemfire.internal.LocalLogWriter;
import com.gemstone.gemfire.internal.LogWriterImpl;

/**
 * Centralizes log configuration and initialization.  This class is a placeholder
 * waiting to be replaced by a more advanced framework like SL4J + Logback.
 * 
 * @author bakera
 */
public class LogService {
  
  /** the configured logger */
  private static LogWriterI18n logger;
  
  /**
   * Configures the logging service.  If a log writer is not supplied, a default
   * log writer will be created that sends log messages to the console.  The
   * default log level can be controlled by setting the <code>log-level</code>
   * system property.
   * 
   * @param logWriter the gemfire log destination
   * @return the log destination
   */
  public static LogWriterI18n configure(LogWriterI18n logWriter) {
    if (logger != null) {
      return logger;
    }

    // try to get the logger from the cache
    if (logWriter == null) {
      logWriter = InternalDistributedSystem.getLoggerI18n();
    }
    
    // provide a default configuration if needed
    if (logWriter == null) {
      int level = LogWriterImpl.FINE_LEVEL;
      try {
        level = LogWriterImpl.levelNameToCode(System.getProperty("log-level"));
      } catch (Exception e) {
      }
      logWriter = new LocalLogWriter(level);
    }
    logger = logWriter;

    // forwards log events to the gemfire logger, assumes commons-logging is using
    // the JDK14 logger implementation
    configureHandler(logWriter);
    
    return logger;
  }
 
  /**
   * Returns the configured log writer.
   * @return the logger
   */
  public static LogWriterI18n logger() {
    // skipping happens-before means this could happen multiple times
    return configure(InternalDistributedSystem.getLoggerI18n());
  }
  
  public static LogWriter logWriter() {
    return logger().convertToLogWriter();
  }
  
  /**
   * Clears the embedded log reference.
   */
  public static void clear() {
    clearHandler(Logger.getLogger(""));
    logger = null;
  }
  
  private static void configureHandler(LogWriterI18n log) {
    Logger root = Logger.getLogger("");
    clearHandler(root);

    // ensure that the root level matches the gemfire level
    if (!Boolean.getBoolean("gemfire.disable-java-log-level")) {
      root.setLevel(GemFireLevel.create(log));
    }
    root.addHandler(log.getHandler());
  }
  
  private static void clearHandler(Logger root) {
    for (Handler handler : root.getHandlers()) {
      root.removeHandler(handler);
      try {
        handler.close();
      } catch (Exception e) {
      }
    }
  }
}

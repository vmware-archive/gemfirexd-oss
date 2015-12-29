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
package com.gemstone.gemfire.internal;

//import java.io.*;
import java.util.logging.*;
//import java.util.Date;

import com.gemstone.gemfire.GemFireException;

/**
 * Implementation of the standard JDK handler that publishes a log record
 * to a LogWriterImpl.
 * Note this handler ignores any installed handler.
 */
public final class GemFireHandler extends Handler {

  /**
   * Use the log writer to use some of its formatting code.
   */
  private LogWriterImpl logWriter;

  public GemFireHandler(LogWriterImpl logWriter) {
    this.logWriter = logWriter;
    this.setFormatter(new GemFireFormatter(logWriter));
  }

  @Override
  public void close() {
    // clear the reference to GFE LogWriter
    this.logWriter = null;
  }

  @Override
  public void flush() {
    // nothing needed
  }

  private String getMessage(LogRecord record) {
    final StringBuilder b = new StringBuilder();
    b .append('(')
      .append("tid=" + record.getThreadID())
      .append(" msgId=" + record.getSequenceNumber())
      .append(") ");
    if (record.getMessage() != null) {
      b.append(getFormatter().formatMessage(record));
    }
    return b.toString();
  }

  @Override
  public void publish(LogRecord record) {
    if (isLoggable(record)) {
      try {
        LogWriterImpl logger = this.logWriter;
        if (logger == null) {
          return;
        }
        logger.put(record.getLevel().intValue(),
                      getMessage(record),
                      record.getThrown());
      } catch (GemFireException ex) {
        reportError(null, ex, ErrorManager.WRITE_FAILURE);
      }
    }
  }
}

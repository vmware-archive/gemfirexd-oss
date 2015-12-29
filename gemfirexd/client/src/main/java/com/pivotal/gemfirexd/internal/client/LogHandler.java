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

package com.pivotal.gemfirexd.internal.client;

import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;

import com.pivotal.gemfirexd.internal.shared.common.sanity.SanityManager;

/**
 * Implementation of the standard JDK handler that publishes a log record using
 * SanityManager.
 * 
 * Note this handler ignores any installed handler.
 */
public class LogHandler extends Handler {

  public LogHandler(Level level) {
    this.setLevel(level);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void publish(LogRecord record) {
    final String msg;
    if (isLoggable(record) && (msg = record.getMessage()) != null) {
      SanityManager.DEBUG_PRINT(record.getLevel().toString().toLowerCase()
          + ":Record=" + record.getSequenceNumber(), msg, record.getThrown());
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void flush() {
    // nothing needed
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void close() {
    // nothing needed
  }
}

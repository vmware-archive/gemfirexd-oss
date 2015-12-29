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

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.org.jgroups.util.StringId;

/**
 * A log writer for security related logs. This will prefix all messages with
 * "security-" in the level part of log-line for easy recognition and filtering
 * if required. Intended usage is in all places where security related logging
 * (authentication, authorization success and failure of clients and peers) as
 * well as for security callbacks.
 * 
 * This class wraps an existing {@link LogWriterImpl} object to add the
 * security prefix feature mentioned above.
 * 
 * @author Sumedh Wale
 * @since 5.5
 */
public final class SecurityLogWriter extends LogWriterImpl implements LogWriterI18n {

  public static final String SECURITY_PREFIX = "security-";
  
  private final int logLevel;

  private final LogWriterImpl realLogWriter;

  public SecurityLogWriter(int level, LogWriter logger) {

    this.logLevel = level;
    this.realLogWriter = (LogWriterImpl) logger;
  }

  @Override
  public int getLevel() {
    return this.logLevel;
  }
 
  /**
   * Handles internationalized log messages.
   * @param params each Object has toString() called and substituted into the msg
   * @see StringId
   * @since 6.0 
   */
  @Override
  protected void put(int msgLevel, StringId msgId, Object[] params, Throwable exception) {
    final String msg = msgId.toLocalizedString(params);
    this.realLogWriter.put(msgLevel + SECURITY_LOGGING_FLAG, msg, exception);
  }  
  @Override
  protected void put(int msgLevel, String msg, Throwable exception) {
    this.realLogWriter.put(msgLevel + SECURITY_LOGGING_FLAG, msg, exception);
  }
}

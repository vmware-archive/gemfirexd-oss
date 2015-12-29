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
package com.gemstone.gemfire.internal.security;

import java.io.PrintStream;

import com.gemstone.gemfire.internal.PureLogWriter;
import com.gemstone.gemfire.internal.SecurityLogWriter;

/**
 * Implementation of {@link com.gemstone.gemfire.LogWriter} that will write
 * security related logs to a local stream.
 * 
 * @author Sumedh Wale
 * @since 5.5
 */
public class SecurityLocalLogWriter extends PureLogWriter {

  /**
   * Creates a writer that logs to given <code>{@link PrintStream}</code>.
   * 
   * @param level
   *                only messages greater than or equal to this value will be
   *                logged.
   * @param logWriter
   *                is the stream that message will be printed to.
   * 
   * @throws IllegalArgumentException
   *                 if level is not in legal range
   */
  public SecurityLocalLogWriter(int level, PrintStream logWriter) {
    super(level, logWriter);
  }

  /**
   * Creates a writer that logs to given <code>{@link PrintStream}</code> and
   * having the given <code>connectionName</code>.
   * 
   * @param level
   *                only messages greater than or equal to this value will be
   *                logged.
   * @param logWriter
   *                is the stream that message will be printed to.
   * @param connectionName
   *                Name of connection associated with this log writer
   * 
   * @throws IllegalArgumentException
   *                 if level is not in legal range
   */
  public SecurityLocalLogWriter(int level, PrintStream logWriter,
      String connectionName) {
    super(level, logWriter, connectionName);
  }

  /**
   * Adds the {@link SecurityLogWriter#SECURITY_PREFIX} prefix to the log-level
   * to distinguish security related log-lines.
   */
  @Override
  protected void put(int msgLevel, String msg, Throwable exception) {
    final String levelStr = SecurityLogWriter.SECURITY_PREFIX + levelToString(msgLevel);
    super.put(msgLevel, levelStr + msg, exception);
  }

}

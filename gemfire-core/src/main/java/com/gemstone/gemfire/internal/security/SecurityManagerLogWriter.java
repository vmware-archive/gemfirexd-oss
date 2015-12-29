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

import java.io.File;
import java.io.PrintStream;

import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.ManagerLogWriter;
import com.gemstone.gemfire.internal.SecurityLogWriter;

/**
 * A log writer for security related logs. This will prefix all messages with
 * "security-" in the level part of log-line for easy recognition and filtering
 * if required. Intended usage is in all places where security related logging
 * (authentication, authorization success and failure of clients and peers) as
 * well as for security callbacks.
 * 
 * This class extends the {@link ManagerLogWriter} to add the security prefix
 * feature mentioned above.
 * 
 * @author Sumedh Wale
 * @since 5.5
 */
public final class SecurityManagerLogWriter extends ManagerLogWriter {

  public SecurityManagerLogWriter(int level, PrintStream stream) {

    super(level, stream);
  }

  public SecurityManagerLogWriter(int level, PrintStream stream,
      String connectionName) {

    super(level, stream, connectionName);
  }

  @Override
  public void setConfig(LogConfig config) {

    if (config instanceof DistributionConfig) {
      config = new SecurityLogConfig((DistributionConfig)config);
    }
    super.setConfig(config);
  }

  /**
   * Adds the {@link SecurityLogWriter#SECURITY_PREFIX} prefix to the log-level
   * to distinguish security related log-lines.
   */
  @Override
  protected void put(int msgLevel, String msg, Throwable exception) {
    super.put(msgLevel, SecurityLogWriter.SECURITY_PREFIX + levelToString(msgLevel) +" "+ msg,
        exception);
  }

  public static class SecurityLogConfig implements LogConfig {

    DistributionConfig config;

    public SecurityLogConfig(DistributionConfig config) {
      this.config = config;
    }

    public int getLogLevel() {
      return this.config.getSecurityLogLevel();
    }

    public File getLogFile() {
      return this.config.getSecurityLogFile();
    }

    public int getLogFileSizeLimit() {
      return this.config.getLogFileSizeLimit();
    }

    public int getLogDiskSpaceLimit() {
      return this.config.getLogDiskSpaceLimit();
    }

  }

}

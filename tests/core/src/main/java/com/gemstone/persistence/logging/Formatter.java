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
package com.gemstone.persistence.logging;

/**
 * Abstract class that formats LogRecords
 */
public abstract class Formatter {

  /** Should we print a stack trace along with logging messages */
  protected static boolean STACK_TRACE =
    Boolean.getBoolean("com.gemstone.persistence.logging.StackTraces");

  /**
   * Formats the given log record as a String
   */
  public abstract String format(LogRecord record);

  /**
   * Formats the message string from a log record
   */
  public String formatMessage(LogRecord record) {
    // Simple
    return(record.getMessage());
  }

}

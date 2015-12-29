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


import com.gemstone.org.jgroups.util.StringId;

/**
 * A log writer that logs all types of log messages as a warning.
 * Intended usage was for individual classes that had their
 * own logger reference and switched it for debugging purposes.
 * e.g.
 *  <pre>
 *  Foo() { // constructor for class Foo
 *    if (Boolean.getBoolean(getClass().getName() + "-logging")) {
 *      this.logger = new DebugLogWriter((LogWriterImpl) getCache().getLogger(), getClass());
 *    } else {
 *      this.logger = pr.getCache().getLogger();
 *    }
 *  }
 *  </pre>
 * 
 * @author Mitch Thomas
 * @since 5.0
 */
final public class DebugLogWriter extends LogWriterImpl
{
  private final LogWriterImpl realLogWriter;
  private final String prefix;
  public DebugLogWriter(LogWriterImpl lw, Class c) {
    this.realLogWriter = lw;
    this.prefix = c.getName() + ":";
//    this.realLogWriter.config(LocalizedStrings.DebugLogWriter_STARTED_USING_CLASS_LOGGER_FOR__0, getClass().getName());
  }

  @Override
  public int getLevel()
  {
    return ALL_LEVEL;
  }

  @Override
  protected void put(int level, String msg, Throwable exception) {
    this.realLogWriter.put(WARNING_LEVEL, this.prefix + " level " + levelToString(level)
        + " " + msg, exception);
  }

  /**
   * Handles internationalized log messages.
   * @param params each Object has toString() called and substituted into the msg
   * @see com.gemstone.org.jgroups.util.StringId
   * @since 6.0 
   */
  @Override
  protected void put(int msgLevel, StringId msgId, Object[] params, Throwable exception)
  {
    String msg = this.prefix + " level " + levelToString(msgLevel) + " " 
           + msgId.toLocalizedString(params);
    this.realLogWriter.put(WARNING_LEVEL, msg, exception);
  }

  @Override
  public boolean configEnabled()
  {
    return true;
  }

  @Override
  public boolean fineEnabled()
  {
    return true;
  }

  @Override
  public boolean finerEnabled()
  {
    return true;
  }

  @Override
  public boolean finestEnabled()
  {
    return true;
  }

  @Override
  public boolean infoEnabled()
  {
    return true;
  }

  @Override
  public boolean severeEnabled()
  {
    return true;
  }

  @Override
  public boolean warningEnabled()
  {
    return true;
  }
  
  
  
}

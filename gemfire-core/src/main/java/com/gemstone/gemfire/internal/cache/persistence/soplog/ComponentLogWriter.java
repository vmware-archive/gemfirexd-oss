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
package com.gemstone.gemfire.internal.cache.persistence.soplog;

import java.util.logging.Handler;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.org.jgroups.util.StringId;

/**
 * Utility log writer class to enable verbose logging for individual components.
 * It promotes log-level for specified log messages of a component to info
 * level. This avoid the need to change entire system's log level which causes
 * performance overheads.
 * 
 * <UL>
 * <li>System log level is set to info
 * <li>To promote all fine level logs of soplog component, add a system property
 * log-level.soplog=FINE
 * <li>Create instance of this class and provide an optional prefix. this prefix
 * will appear in all log messages
 * 
 * @author bakera
 */
public class ComponentLogWriter implements LogWriterI18n {
  private final int COMPONENT_LOG_LEVEL;

  private final LogWriterI18n log;
  private final String prefix;
  private final String name;

  private enum LogLevel {
    FINEST(10), 
    FINER(20),
    FINE(30),
    CONFIG(40),
    INFO(50),
    WARNING(60), 
    ERROR(70), 
    SEVERE(80) 
    ;

    int value;

    LogLevel(int value) {
      this.value = value;
    }
  }

  /**
   * @param prefix String to be prefixed in all log messages
   * @param delegate all logging requests will be delegated to this instance
   * @return instance of component log writer for soplog component
   */
  public static ComponentLogWriter getSoplogLogWriter(String prefix, LogWriterI18n delegate) {
    return new ComponentLogWriter("Soplog", prefix, delegate);
  }

  /**
   * @param prefix String to be prefixed in all log messages
   * @param delegate all logging requests will be delegated to this instance
   * @return instance of component log writer for soplog component
   */
  public static ComponentLogWriter getHoplogLogWriter(String prefix, LogWriterI18n delegate) {
    return new ComponentLogWriter("Hoplog", prefix, delegate);
  }
  
  public ComponentLogWriter(String componentName, String logPrefix, LogWriterI18n delegate) {
    this.log = delegate;
    this.name = logPrefix;

    this.prefix = "<" + componentName + ":" + logPrefix + "> ";
    
    int log_level_value = LogLevel.SEVERE.value;
    
    try {
      log_level_value = LogLevel.valueOf(System.getProperty("log-level." + componentName)).value;
    } catch (IllegalArgumentException e) {
    } catch (NullPointerException e) {
    }
    COMPONENT_LOG_LEVEL = log_level_value;
  }

  private boolean isAtLeast(LogLevel logLevel) {
    return COMPONENT_LOG_LEVEL <= logLevel.value;
  }
  
  public String prefix() {
    return prefix;
  }
  
  public String name() {
    return name;
  }
  
  private String prepend(String msg) {
    return prefix + msg;
  }
  
  @Override
  public boolean severeEnabled() {
    return log.severeEnabled();
  }

  @Override
  public void severe(Throwable ex) {
    log.severe(ex);
  }

  @Override
  public void severe(StringId msgID, Object[] params, Throwable ex) {
    log.severe(msgID, params, ex);
  }

  @Override
  public void severe(StringId msgID, Object param, Throwable ex) {
    log.severe(msgID, param, ex);
  }

  @Override
  public void severe(StringId msgID, Throwable ex) {
    log.severe(msgID, ex);
  }

  @Override
  public void severe(StringId msgID, Object[] params) {
    log.severe(msgID, params);
  }

  @Override
  public void severe(StringId msgID, Object param) {
    log.severe(msgID, param);
  }

  @Override
  public void severe(StringId msgID) {
    log.severe(msgID);
  }

  @Override
  public boolean errorEnabled() {
    return log.errorEnabled();
  }

  @Override
  public void error(Throwable ex) {
    log.error(ex);
  }

  @Override
  public void error(StringId msgID, Object[] params, Throwable ex) {
    log.error(msgID, params, ex);
  }

  @Override
  public void error(StringId msgID, Object param, Throwable ex) {
    log.error(msgID, param, ex);
  }

  @Override
  public void error(StringId msgID, Throwable ex) {
    log.error(msgID, ex);
  }

  @Override
  public void error(StringId msgID, Object[] params) {
    log.error(msgID, params);
  }

  @Override
  public void error(StringId msgID, Object param) {
    log.error(msgID, param);
  }

  @Override
  public void error(StringId msgID) {
    log.error(msgID);
  }

  @Override
  public boolean warningEnabled() {
    return log.warningEnabled();
  }

  @Override
  public void warning(Throwable ex) {
    log.warning(ex);
  }

  @Override
  public void warning(StringId msgID, Object[] params, Throwable ex) {
    log.warning(msgID, params, ex);
  }

  @Override
  public void warning(StringId msgID, Object param, Throwable ex) {
    log.warning(msgID, param, ex);
  }

  @Override
  public void warning(StringId msgID, Throwable ex) {
    log.warning(msgID, ex);
  }

  @Override
  public void warning(StringId msgID, Object[] params) {
    log.warning(msgID, params);
  }

  @Override
  public void warning(StringId msgID, Object param) {
    log.warning(msgID, param);
  }

  @Override
  public void warning(StringId msgID) {
    log.warning(msgID);
  }

  @Override
  public boolean infoEnabled() {
    return log.infoEnabled();
  }

  @Override
  public void info(Throwable ex) {
    log.info(ex);
  }

  @Override
  public void info(StringId msgID, Object[] params, Throwable ex) {
    log.info(msgID, params, ex);
  }

  @Override
  public void info(StringId msgID, Object param, Throwable ex) {
    log.info(msgID, param, ex);
  }

  @Override
  public void info(StringId msgID, Throwable ex) {
    log.info(msgID, ex);
  }

  @Override
  public void info(StringId msgID, Object[] params) {
    log.info(msgID, params);
  }

  @Override
  public void info(StringId msgID, Object param) {
    log.info(msgID, param);
  }

  @Override
  public void info(StringId msgID) {
    log.info(msgID);
  }

  @Override
  public boolean configEnabled() {
    return log.configEnabled();
  }

  @Override
  public void config(Throwable ex) {
    log.config(ex);
  }

  @Override
  public void config(StringId msgID, Object[] params, Throwable ex) {
    log.config(msgID, params, ex);
  }

  @Override
  public void config(StringId msgID, Object param, Throwable ex) {
    log.config(msgID, param, ex);
  }

  @Override
  public void config(StringId msgID, Throwable ex) {
    log.config(msgID, ex);
  }

  @Override
  public void config(StringId msgID, Object[] params) {
    log.config(msgID, params);
  }

  @Override
  public void config(StringId msgID, Object param) {
    log.config(msgID, param);
  }

  @Override
  public void config(StringId msgID) {
    log.config(msgID);
  }

  @Override
  public boolean fineEnabled() {
    return log.fineEnabled() || isAtLeast(LogLevel.FINE);
  }

  @Override
  public void fine(String msg, Throwable ex) {
    msg = prepend(msg);
    if (isAtLeast(LogLevel.FINE)) {
      info(LocalizedStrings.ONE_ARG, msg, ex);
    } else {
      log.fine(msg, ex);
    }
  }

  @Override
  public void fine(String msg) {
    msg = prepend(msg);
    if (isAtLeast(LogLevel.FINE)) {
      info(LocalizedStrings.ONE_ARG, msg);
    } else {
      log.fine(msg);
    }
  }

  @Override
  public void fine(Throwable ex) {
    if (isAtLeast(LogLevel.FINE)) {
      info(ex);
    } else {
      log.fine(ex);
    }
  }

  @Override
  public boolean finerEnabled() {
    return log.finerEnabled() || (isAtLeast(LogLevel.FINER));
  }

  @Override
  public void finer(String msg, Throwable ex) {
    msg = prepend(msg);
    if (isAtLeast(LogLevel.FINER)) {
      info(LocalizedStrings.ONE_ARG, msg, ex);
    } else {
      log.finer(msg, ex);
    }
  }

  @Override
  public void finer(String msg) {
    msg = prepend(msg);
    if (isAtLeast(LogLevel.FINER)) {
      info(LocalizedStrings.ONE_ARG, msg);
    } else {
      log.finer(msg);
    } 
  }

  @Override
  public void finer(Throwable ex) {
    if (isAtLeast(LogLevel.FINER)) {
      info(ex);
    } else {
      log.finer(ex);
    } 
  }

  @Override
  public void entering(String sourceClass, String sourceMethod) {
    log.entering(sourceClass, sourceMethod);
  }

  @Override
  public void exiting(String sourceClass, String sourceMethod) {
    log.exiting(sourceClass, sourceMethod);
  }

  @Override
  public void throwing(String sourceClass, String sourceMethod, Throwable thrown) {
    log.throwing(sourceClass, sourceMethod, thrown);
  }

  @Override
  public boolean finestEnabled() {
    return log.finestEnabled() || (isAtLeast(LogLevel.FINEST));
  }

  @Override
  public void finest(String msg, Throwable ex) {
    msg = prepend(msg);
    if (isAtLeast(LogLevel.FINEST)) {
      info(LocalizedStrings.ONE_ARG, msg, ex);
    } else {
      log.finest(msg, ex);
    }
  }

  @Override
  public void finest(String msg) {
    msg = prepend(msg);
    if (isAtLeast(LogLevel.FINEST)) {
      info(LocalizedStrings.ONE_ARG, msg);
    } else {
      log.finest(msg);
    }
  }

  @Override
  public void finest(Throwable ex) {
    if (isAtLeast(LogLevel.FINEST)) {
      info(ex);
    } else {
      log.finest(ex);
    } 
  }

  @Override
  public Handler getHandler() {
    return log.getHandler();
  }

  @Override
  public LogWriter convertToLogWriter() {
    return log.convertToLogWriter();
  }
}

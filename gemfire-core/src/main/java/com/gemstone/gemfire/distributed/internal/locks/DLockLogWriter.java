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

package com.gemstone.gemfire.distributed.internal.locks;

import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.*;
import com.gemstone.gemfire.distributed.internal.DM;

/**
 * LogWriterI18n which forces fineEnabled to true if DistributedLockService.VERBOSE
 * is true.
 *
 * @author Kirk Lund
 */
public class DLockLogWriter extends VerboseLogWriter  {
   
  /** Should we log operations related to DistributedLockService? */
  public static final boolean VERBOSE = Boolean.getBoolean("DistributedLockService.VERBOSE");

  /** True will suppress fine-level logging for DLS */
  public static final boolean SUPPRESS_FINE = Boolean.getBoolean("DistributedLockService.SUPPRESS_FINE");
  
  private final String dlsName;
  
  public DLockLogWriter(LogWriterI18n delegate) {
    super(delegate);
    this.dlsName = "";
  }
  
  public DLockLogWriter(String dlsName, int dlsSerialNumber, LogWriterI18n delegate) {
    super(delegate);
    this.dlsName = "<" + dlsName + ":" + dlsSerialNumber + "> ";
  }
  
  public DLockLogWriter(String dlsName, LogWriterI18n delegate) {
    super(delegate);
    this.dlsName = "<" + dlsName + "> ";
  }
  
  @Override
  public void put(int msgLevel, String msg, Throwable exception) {
    super.put(msgLevel, this.dlsName + msg, exception);
  }
  
  @Override
  protected boolean isVerbose() {
    return VERBOSE;
  }
  
  public static boolean fineEnabled(DM dm) {
    return !SUPPRESS_FINE && dm.getLoggerI18n().fineEnabled() || VERBOSE;
  }
  
  public static void fine(DM dm, String msg) {
    if (SUPPRESS_FINE) {
      return;
    }
    if (VERBOSE && !dm.getLoggerI18n().fineEnabled()) {
      dm.getLoggerI18n().info(LocalizedStrings.ONE_ARG, msg);
    }
    else {
      dm.getLoggerI18n().fine(msg);
    }
  }
  
  public static void fine(DM dm, String msg, Throwable ex) {
    if (SUPPRESS_FINE) {
      return;
    }
    if (VERBOSE && !dm.getLoggerI18n().fineEnabled()) {
      dm.getLoggerI18n().info(LocalizedStrings.ONE_ARG, msg, ex);
    }
    else {
      dm.getLoggerI18n().fine(msg, ex);
    }
  }

  public static boolean fineEnabled(LogWriterI18n log) {
    return !SUPPRESS_FINE && log.fineEnabled() || VERBOSE;
  }
  
  public static void fine(LogWriterI18n log, String msg) {
    if (SUPPRESS_FINE) {
      return;
    }
    if (VERBOSE && !log.fineEnabled()) {
      log.info(LocalizedStrings.ONE_ARG,msg);
    }
    else {
      log.fine(msg);
    }
  }
  
  public static void fine(LogWriterI18n log, String msg, Throwable ex) {
    if (SUPPRESS_FINE) {
      return;
    }
    if (VERBOSE && !log.fineEnabled()) {
      log.info(LocalizedStrings.ONE_ARG, msg, ex);
    }
    else {
      log.fine(msg, ex);
    }
  }

  @Override
  public boolean fineEnabled() {
    return !SUPPRESS_FINE && getLevel() <= FINE_LEVEL || isVerbose();
  }
}


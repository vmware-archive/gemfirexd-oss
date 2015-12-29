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

import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.org.jgroups.util.StringId;


/**
 * LogWriterI18n which forces fineEnabled to true if isVerbose is true.
 *
 * @author Kirk Lund
 */
public abstract class VerboseLogWriter extends LogWriterImpl {
   
  /** Underlying <code>LogWriterI18n</code> to delegate to */
  protected final LogWriterImpl delegate;
   
  /**
   * Creates a writer that delegates to an underlying <code>LogWriterI18n</code>.
   * @param delegate underlying <code>LogWriterI18n</code> to delegate to
   * @throws IllegalArgumentException if delegate is null
   */
  public VerboseLogWriter(LogWriterI18n delegate) {
    if (delegate == null) {
      throw new IllegalArgumentException(LocalizedStrings.VerboseLogWriter_VERBOSELOGWRITER_REQUIRES_DELEGATE_LOGWRITERI18N.toLocalizedString());
    }
    this.delegate = (LogWriterImpl) delegate;
  }
  
  // override these methods to use VERBOSE
  
  @Override
  public boolean severeEnabled() {
    return this.getLevel() <= SEVERE_LEVEL || isVerbose();
  }
  @Override
  public boolean errorEnabled() {
    return this.getLevel() <= ERROR_LEVEL || isVerbose();
  }
  @Override
  public boolean warningEnabled() {
    return this.getLevel() <= WARNING_LEVEL || isVerbose();
  }
  @Override
  public boolean infoEnabled() {
    return this.getLevel() <= INFO_LEVEL || isVerbose();
  }
  @Override
  public boolean configEnabled() {
    return this.getLevel() <= CONFIG_LEVEL || isVerbose();
  }
  @Override
  public boolean fineEnabled() {
    return this.getLevel() <= FINE_LEVEL || isVerbose();
  }
  @Override
  public boolean finerEnabled() {
    return this.getLevel() <= FINER_LEVEL || isVerbose();
  }
  @Override
  public boolean finestEnabled() {
    return this.getLevel() <= FINEST_LEVEL || isVerbose();
  }

  // implement these methods to use the underlying delegate
  
  @Override
  public int getLevel() {
    return this.delegate.getLevel();
  }

  @Override
  public void put(int level, String msg, Throwable exception) {
    this.delegate.put(level, msg, exception);
  }

  /**
   * @since 6.0 
   */
  @Override
  public void put(int msgLevel, StringId msgId, Object[] params,
         Throwable exception) {
    this.delegate.put(msgLevel, msgId, params, exception);
  }
  
  protected abstract boolean isVerbose();
}


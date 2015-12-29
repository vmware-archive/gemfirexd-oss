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

package com.pivotal.gemfirexd.internal.shared.common.sanity;

/**
 * An extension to <code>HeaderPrintWriter</code> to enable writing GemFire
 * style logs.
 * 
 * @author swale
 */
public interface GfxdHeaderPrintWriter {

  /**
   * @see PureLogWriter#put(int, String, Throwable)
   */
  public void put(String flag, String message, Throwable t);

  /**
   * Refresh the given debug flag to enable/disable it. Not supposed to be
   * thread-safe.
   * 
   * @param debugFlag
   *          the debug flag to enable/disable; a null value indicates all debug
   *          flags
   * @param enable
   *          true to enable the flag and false to disable it
   * 
   * @see SanityManager#DEBUG_ON(String)
   * @see SanityManager#DEBUG_CLEAR(String)
   */
  public void refreshDebugFlag(String debugFlag, boolean enable);

  /**
   * Used to determine fine level of information is to be logged or not.
   */
  public boolean isFineEnabled();

  /**
   * Used to determine finer level of information is to be logged or not.
   */
  public boolean isFinerEnabled();
}

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

import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;

/**
 * Used to determine if product should use pure java mode.
 */
public final class PureJavaMode {
  /**
   * System property to set to true to force pure java mode
   */
  public final static String PURE_MODE_PROPERTY = "gemfire.pureJavaMode";
  private static final boolean isPure;
  private static final boolean is64Bit;
  private static final boolean osStatsAreAvailable;
  static {
    boolean tmpIsPure = false;
    if (Boolean.getBoolean(PURE_MODE_PROPERTY)) {
      if (SharedLibrary.debug) {
        System.out.println("property " + PURE_MODE_PROPERTY + " is true");
      }
      tmpIsPure = true;
    } else {
      System.out.println("SS GFCI.gfxdSystem() " + GemFireCacheImpl.gfxdSystem());
      tmpIsPure = GemFireCacheImpl.gfxdSystem() //don't load gemfire native library for now as we don't supply one.
          || !SharedLibrary.loadLibrary(SharedLibrary.getName("gemfire"));
    }
    isPure = tmpIsPure;
    is64Bit = SharedLibrary.is64Bit();
    String osName = System.getProperty("os.name", "unknown");
    osStatsAreAvailable = osName.startsWith("Linux") || ! isPure; 
  }

  public final static boolean isPure() {
    return isPure;
  }
  public final static boolean is64Bit() {
    return is64Bit;
  }
  /**
   * Linux has OsStats even in PureJava mode but other platforms
   * require the native code to provide OS Statistics.
   * return true if OSStatistics are available
   */
  public final static boolean osStatsAreAvailable() {
    return osStatsAreAvailable;
  }
}

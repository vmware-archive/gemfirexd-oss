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

package cacheperf.comparisons.replicated.cq;

import cacheperf.CachePerfException;

/**
 * Functions for use in test configuration files.  All functions must return
 * Strings suitable for further parsing by hydra.
 */
public class Fcns {

  /**
   * Returns the Xms and Xmx heap specifications as the specified percentage
   * of the given heap size plus 100 MB to grow on, duplicated n times.
   */
  public static String duplicateHeapSize(int heapMB, int heapPercent, int n) {
    if (heapMB <= 0) {
      String s = "Illegal heap MB: " + heapMB;
      throw new CachePerfException(s);
    }
    if (heapPercent <= 0 || heapPercent > 100) {
      String s = "Illegal heap percent: " + heapPercent;
      throw new CachePerfException(s);
    }
    int heapSize = (int)Math.ceil((heapPercent/100.0) * heapMB) + 100;
    String heapString = "-Xms" + heapSize + "m -Xmx" + heapSize + "m ";
    StringBuffer buf = new StringBuffer();
    for (int i = 0; i < n; i++) {
      buf.append(heapString);
    }
    return buf.toString().trim();
  }
}

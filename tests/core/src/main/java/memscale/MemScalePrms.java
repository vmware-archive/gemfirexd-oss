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
package memscale;

import hydra.BasePrms;

import java.util.Vector;

public class MemScalePrms extends BasePrms {

  /**
   * (int) The number of times the test should execute its main task before terminating.
   */
  public static Long numberExecutionCycles;
  public static int getNumberExecutionCycles() {
    Long key = numberExecutionCycles;
    int value = tasktab().intAt(key, tab().intAt(key, 0));
    return value;
  }

  /**
   * (boolean) When the task is destroy and the keyPercentage is set to 100, use region clear()
   *           rather than individually destroying every entry.
   */
  public static Long useClear;
  public static boolean getUseClear() {
    Long key = useClear;
    boolean value = tasktab().booleanAt(key, tab().booleanAt(key, false));
    return value;
  }
  
  /**
   *  (Vector) A list of maxKeys to use after the initial and original setting of CachePerfPrms.maxKeys.
   *           This is parallel to alternateObjectSizes.
   */
  public static Long alternateMaxKeys;
  public static Vector getAlternateMaxKeys() {
    Long key = alternateMaxKeys;
    Vector value = tab().vecAt(key, null);
    return value;
  }

  /**
   *  (Vector) A list of objectSizes to use after the initial and original setting of objects.ArrayOfBytePrms.size.
   *           This is parallel to alternateMaxKeys.
   */
  public static Long alternateObjectSizes;
  public static Vector getAlternateObjectSizes() {
    Long key = alternateObjectSizes;
    Vector value = tab().vecAt(key, null);
    return value;
  }

  /**
   *  (int) The number of members of interest in a test. This is calculated in the config and is not
   *        intended to be expicitly set.
   */
  public static Long numMembers;
  public static int getNumMembers() {
    Long key = numMembers;
    int value = tab().intAt(key, 0);
    return value;
  }
  
  /**
   *  (int) The target counter value for verify off-heap memory to wait for to coordinate
   *        quietness in the system when off-heap validation is running.
   */
  public static Long offHeapVerifyTargetCount;
  public static int getOffHeapVerifyTargetCount() {
    Long key = offHeapVerifyTargetCount;
    int value = tab().intAt(key, 0);
    return value;
  }
  
  /**
   *  (int) The number of threads of interest in a test. This is calculated in the config and is not
   *        intended to be expicitly set.
   */
  public static Long numThreads1;
  public static int getNumThreads1() {
    Long key = numThreads1;
    int value = tab().intAt(key, 0);
    return value;
  }
  
  /**
   *  (int) The number of threads of interest in a test. This is calculated in the config and is not
   *        intended to be expicitly set.
   */
  public static Long numThreads2;
  public static int getNumThreads2() {
    Long key = numThreads2;
    int value = tab().intAt(key, 0);
    return value;
  }
  
// ================================================================================
static {
   BasePrms.setValues(MemScalePrms.class);
}

}
